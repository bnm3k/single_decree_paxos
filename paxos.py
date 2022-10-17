from typing import Protocol, NamedTuple, Optional, Union


class ProposalNumber(NamedTuple):
    """
    Proposal

    In order for the paxos algorithm to function, all proposal numbers must be
    unique. A simple way to ensure this is to include the proposer's unique
    server ID in the proposal id.

    Named tuples allow the proposal number and server_id to be combined in a manner
    that supports comparison in the expected manner:

        (4, 'C') > (4, 'B') > (3, Z)
    """

    number: int
    server_id: str

    @staticmethod
    def min() -> "ProposalNumber":
        return ProposalNumber(-1, "")


class PrepareMessage(NamedTuple):
    """
    Prepare messages should be broadcast to all Acceptors.
    """

    proposal_number: ProposalNumber


class PromiseMessage(NamedTuple):
    """
    Sent by an acceptor back to a proposer. If an acceptor has already accepted
    a value, it sets the proposal number and value
    """

    from_server_id: str  # acceptor's server_id, in case message is duplicated
    accepted_proposal_number: Optional[ProposalNumber]
    accepted_value: Optional[str]


class AcceptMessage(NamedTuple):
    """
    Accept message should be broadcast to all Acceptors
    """

    proposal_number: ProposalNumber
    value: str


class AcceptedMessage(NamedTuple):

    from_server_id: str  # acceptor's server_id, in case message is duplicated
    proposal_number: ProposalNumber


PaxosMessage = Union[PrepareMessage, PromiseMessage, AcceptMessage, AcceptedMessage]


class InvalidMessageError(Exception):
    """
    Thrown if a PaxosMessage subclass is passed to a class that does not
    support it
    """


class MessageHandler(Protocol):
    _handlers: dict

    def handle_message(self, msg: PaxosMessage):
        """
        Message dispatching function. This function accepts any PaxosMessage
        and calls the appropriate handler function
        """

        message_type = msg.__class__.__name__
        handle_fn = self._handlers.get(message_type)
        if handle_fn is None:
            raise InvalidMessageError(
                f"Receiving class: {self.__class__.__name__} does not support messages of type: {msg.__class__.__name__}"
            )
        return handle_fn(msg)


class Acceptor(MessageHandler):
    server_id: str
    min_proposal_number: ProposalNumber
    accepted_proposal_number: Optional[ProposalNumber] = None
    accepted_value: Optional[str] = None

    def __init__(self, server_id):
        self.server_id = server_id
        self.min_proposal_number = ProposalNumber.min()
        self._handlers = {
            PrepareMessage.__name__: self.on_prepare_message,
            AcceptMessage.__name__: self.on_accept_message,
        }

    def on_prepare_message(self, msg: PrepareMessage) -> PromiseMessage:
        """
        Goal:
            - Proposers should find out about any chosen value
            - Proposers block older proposals that have not yet completed
        """
        if msg.proposal_number >= self.min_proposal_number:
            # With this step, the acceptor promises never to accept a proposal
            # with number less than the one in the incoming request.
            self.min_proposal_number = msg.proposal_number

        return PromiseMessage(
            self.server_id, self.accepted_proposal_number, self.accepted_value
        )

    def on_accept_message(self, msg: AcceptMessage) -> AcceptedMessage:
        if msg.proposal_number >= self.min_proposal_number:
            self.accepted_proposal_number = msg.proposal_number  # should be persisted
            self.accepted_value = msg.value  # TODO should be persisted
        return AcceptedMessage(self.server_id, self.min_proposal_number)


class Proposer(MessageHandler):
    """
    Proposers must persist max_round_number.
    Proposers must not reuse proposal IDs after crash/restart
    """

    server_id: str
    curr_proposed_value: Optional[str] = None
    curr_proposed_number: Optional[ProposalNumber]
    chosen_value: Optional[str] = None  # Acceptor acts as listener
    max_round_number: int
    quorum_size: int

    def __init__(self, server_id: str, num_acceptors: int):
        self.server_id = server_id
        if num_acceptors < 3:
            # TODO, maybe can use 2 acceptors with quorum size of 2
            raise Exception(f"Invalid num of acceptors {num_acceptors}. Should be >= 3")
        self.quorum_size = (num_acceptors // 2) + 1  # quorum size is simple majority
        self.max_round_number = 0
        self._handlers = {
            PromiseMessage.__name__: self.on_promise_message,
            AcceptedMessage.__name__: self.on_accepted_message,
        }
        self._curr_promises: set[PromiseMessage] = set()
        self._curr_accepted: set[AcceptedMessage] = set()

    def propose_value(self, value: str):
        """
        sent by clients,
        """
        assert (
            self.curr_proposed_value is None
        ), "Proposer already has proposed value set"
        self.curr_proposed_value = value

    def get_prepare_message(self) -> PrepareMessage:
        """
        Returns a PrepareRequestMessage with a proposal number higher than that of
        any observed proposals. The PrepareMessage should be broadcasted to all
        Acceptors
        """
        # should be persisted on disk so that we dont reuse it in case of
        # crash/restart
        # TODO  what happens if we reuse a proposal number
        # TODO what if we have a proposal already in flight, do we abandon it
        # and propose the new value?
        self.max_round_number += 1
        self.curr_proposed_number = ProposalNumber(
            self.max_round_number, self.server_id
        )
        return PrepareMessage(self.curr_proposed_number)

    def get_accept_message(self) -> AcceptMessage:
        # TODO don't return an accept message if majority promises have not been returned
        assert (
            self.curr_proposed_number is not None
            and self.curr_proposed_value is not None
        ), "Before getting an  message, a value should have been proposed by a client and majority promises should have been received"
        return AcceptMessage(self.curr_proposed_number, self.curr_proposed_value)

    def on_promise_message(self, msg: PromiseMessage):
        """
        Returns an Accept message if a quorum of PrepareResponse messages is
        achieved. This message should then be broadcast to all acceptors.
        Otherise, returns None
        """

        self._curr_promises.add(msg)
        if len(self._curr_promises) == self.quorum_size:
            # receiving majority responses
            # if no acceptor returned any accepted value, proposer moves on with
            # its initial proposed value, otherwise it uses the value from the
            # highest accepted proposal number
            highest_accepted_proposal_number, accepted_value = (
                ProposalNumber.min(),
                None,
            )
            for res in self._curr_promises:
                if res.accepted_proposal_number is None:
                    assert (
                        res.accepted_value is None
                    ), "If PromiseMessage.accept_proposal_number is None, then PromiseMessage.accepted_value should also be None"
                    continue
                if res.accepted_proposal_number > highest_accepted_proposal_number:
                    highest_accepted_proposal_number = res.accepted_proposal_number
                    assert (
                        res.accepted_value is not None
                    ), "If PromiseMessage.accept_proposal_number is set, then PromiseMessage.accepted_value should also be set"
                    accepted_value = res.accepted_value
            if accepted_value is not None:
                # abandon initially proposed value
                self.curr_proposed_value = accepted_value

    def on_accepted_message(self, msg: AcceptedMessage):
        # check if any rejections, if so, restart process
        # otherwise, a value is chosen
        self._curr_accepted.add(msg)
        if len(self._curr_accepted) == self.quorum_size:
            assert (
                self.curr_proposed_number is not None
            ), "Should run prepare phase first"
            for msg in self._curr_accepted:
                if msg.proposal_number > self.curr_proposed_number:
                    # rejected
                    # TODO, restart proposal again
                    print(msg.proposal_number)
                    self.max_round_number = msg.proposal_number.number
                    raise Exception("Proposal rejected")
                # TODO, broadcast maybe
                self.chosen_value = self.curr_proposed_value
