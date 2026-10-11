from local_finance.ledger import flow_summary


def test_transfers_keep_both_directions_over_long_periods(db) -> None:
    with db.transaction(immediate=True) as connection:
        connection.execute("INSERT INTO accounts(name) VALUES ('A'), ('B')")
        connection.executemany(
            "INSERT INTO transfers(id, date, source_account, target_account, amount) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                ("t1", "2025-01-10", "A", "B", 100),
                ("t2", "2025-08-20", "B", "A", 40),
                ("t3", "2025-08-21", "A", "B", 25),
                ("outside", "2024-12-01", "A", "B", 999),
            ],
        )
    transfers = flow_summary(["2025-01", "2025-08"], db=db)["transfers"]
    links = {(link["source"], link["target"]): link["value"] for link in transfers["links"]}
    assert links == {("A", "B"): 125, ("B", "A"): 40}
    assert {node["name"] for node in transfers["nodes"]} == {"A", "B"}
    january = flow_summary(["2025-01"], db=db)["transfers"]
    assert january["links"] == [{"source": "A", "target": "B", "value": 100}]


def test_empty_month_selection_has_no_transfer_flow(db) -> None:
    assert flow_summary([], db=db)["transfers"] == {"nodes": [], "links": []}
