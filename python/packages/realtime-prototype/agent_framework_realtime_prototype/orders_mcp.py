# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""A tiny orders MCP server (stdio), standing in for the accelerator's orders MCP server."""

from mcp.server.mcpserver import MCPServer

server = MCPServer("orders")
SLOTS = {"S1": "Thursday 9 to 11 AM", "S2": "Thursday 1 to 3 PM", "S3": "Friday 9 to 11 AM"}


@server.tool()
def getDeliverySlots(order_id: str) -> str:  # noqa: N802
    """List the delivery slots available for an order."""
    return f"Order {order_id} can be delivered in slots: " + "; ".join(f"{k}: {v}" for k, v in SLOTS.items())


@server.tool()
def bookDeliverySlot(order_id: str, slot_id: str) -> str:  # noqa: N802
    """Book a delivery slot for an order, by slot ID."""
    if slot_id not in SLOTS:
        return f"Unknown slot {slot_id}."
    return f"Booked order {order_id} for {SLOTS[slot_id]}."


if __name__ == "__main__":
    server.run()
