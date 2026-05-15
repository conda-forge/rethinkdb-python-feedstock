from rethinkdb import r
from rethinkdb.errors import ReqlDriverError


def test_loop_adapters():
    r.set_loop_type("tornado")
    assert r.connection_type.__module__.startswith("rethinkdb.tornado_net")

    r.set_loop_type("trio")
    assert r.connection_type.__module__.startswith("rethinkdb.trio_net")

    r.set_loop_type(None)
    assert r.connection_type.__module__ == "rethinkdb.net"


def test_query_ast_builds_without_server():
    query = (
        r.db("analytics")
        .table("events")
        .filter({"kind": "click"})
        .pluck("id", "kind")
        .order_by(r.asc("id"))
        .limit(2)
    )

    query_text = str(query)
    for token in ("analytics", "events", "filter", "pluck", "limit"):
        assert token in query_text

    compiled = query.build()
    assert isinstance(compiled, list)
    assert len(compiled) >= 2

    literal = r.expr({"ok": True, "values": [1, 2, 3]}).build()
    assert isinstance(literal, dict)
    assert set(literal) == {"ok", "values"}
    assert "values" in str(literal)

    try:
        query.run()
    except ReqlDriverError as exc:
        assert "connection" in str(exc).lower()
    else:
        raise AssertionError("query.run() unexpectedly succeeded without a connection")


if __name__ == "__main__":
    test_loop_adapters()
    test_query_ast_builds_without_server()
