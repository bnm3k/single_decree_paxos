from paxos import *


def test_case_1():
    # set up
    num_acceptors = 3
    acceptors = [Acceptor(f"acceptor-{i}") for i in range(1, num_acceptors + 1)]
    a1, a2, a3 = acceptors[0], acceptors[1], acceptors[2]
    p1 = Proposer("proposer-1", num_acceptors=num_acceptors)
    p2 = Proposer("proposer-2", num_acceptors=num_acceptors)

    # proposer 1 sends prepare messages to acceptors a1 and a2
    p1.propose_value("foo")
    p1_prepare = p1.get_prepare_message()
    a1_promise_msg = a1.handle_message(p1_prepare)
    a2_promise_msg = a2.handle_message(p1_prepare)

    # proposer 1 receives promise messages
    p1.handle_message(a1_promise_msg)
    p1.handle_message(a2_promise_msg)

    # proposer 1 sends accept messages
    p1_accept_message = p1.get_accept_message()
    a1_accepted_msg = a1.handle_message(p1_accept_message)
    a2_accepted_msg = a2.handle_message(p1_accept_message)

    # proposer 1 receives accepted messages
    p1.handle_message(a1_accepted_msg)
    p1.handle_message(a2_accepted_msg)

    # proposer 2 sends prepare message
    p2.propose_value("bar")
    p2_prepare = p2.get_prepare_message()
    a2_promise_msg = a2.handle_message(p2_prepare)
    a3_promise_msg = a3.handle_message(p2_prepare)

    # proposer 2 receives promise messages
    p2.handle_message(a2_promise_msg)
    p2.handle_message(a3_promise_msg)

    # proposer 2 sends accept messages
    p2_accept_message = p2.get_accept_message()
    a2_accepted_msg = a2.handle_message(p2_accept_message)
    a3_accepted_msg = a3.handle_message(p2_accept_message)

    # proposer 1 receives accepted messages
    p2.handle_message(a2_accepted_msg)
    p2.handle_message(a3_accepted_msg)

    assert p2.chosen_value == "foo"
    assert p1.chosen_value == "foo"


def test_case_2():
    # set up
    num_acceptors = 3
    acceptors = [Acceptor(f"acceptor-{i}") for i in range(1, num_acceptors + 1)]
    a1, a2, a3 = acceptors[0], acceptors[1], acceptors[2]
    p1 = Proposer("proposer-1", num_acceptors=num_acceptors)
    p2 = Proposer("proposer-2", num_acceptors=num_acceptors)

    # proposer 1 sends prepare messages to acceptors a1 and a2
    p1.propose_value("foo")
    p1_prepare = p1.get_prepare_message()
    a1_promise_msg_for_p1 = a1.handle_message(p1_prepare)

    # proposer 1 receives promise message from a1
    p1.handle_message(a1_promise_msg_for_p1)

    # proposer 2 sends prepare message
    p2.propose_value("bar")
    p2_prepare = p2.get_prepare_message()
    a2_promise_msg_for_p2 = a2.handle_message(p2_prepare)
    a3_promise_msg_for_p2 = a3.handle_message(p2_prepare)

    # proposer 2 receives promise messages from quorum
    p2.handle_message(a2_promise_msg_for_p2)
    p2.handle_message(a3_promise_msg_for_p2)

    # proposer 2 has received quorum promises, it can now send an accept message
    p2_accept_message = p2.get_accept_message()

    # proposer 2 sends accept message to a3
    a3_accepted_msg_for_p2 = a3.handle_message(p2_accept_message)

    # propose 1 receives promise message from a2
    a2_promise_msg_for_p1 = a2.handle_message(p1_prepare)
    p1.handle_message(a2_promise_msg_for_p1)

    # proposer 1 has received quorum promises, it can now send an accept message
    p1_accept_message = p1.get_accept_message()

    # proposer 1 sends accept message to a1 and a2
    a1_accepted_msg_for_p1 = a1.handle_message(p1_accept_message)
    a2_accepted_msg_for_p1 = a2.handle_message(p1_accept_message)

    # proposer 2 sends accept message to a2
    a2_accepted_msg_for_p2 = a2.handle_message(p2_accept_message)

    # proposer 2 receives accepted messages
    p2.handle_message(a2_accepted_msg_for_p2)
    p2.handle_message(a3_accepted_msg_for_p2)

    # proposer 1 receives accepted messages
    try:
        p1.handle_message(a1_accepted_msg_for_p1)
        p1.handle_message(a2_accepted_msg_for_p1)
    except Exception as e:
        assert p1.chosen_value is None  # p1 should go through process again
        assert str(e) == "Proposal rejected"

    assert p2.chosen_value == "bar"
