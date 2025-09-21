from src.algorithm_base_class.algorithm import Algorithm, Trigger


class TST(Algorithm):
    """Algorithm to showcase TimeSlot functionality."""

    def __init__(self):
        """Constructor."""
        super().__init__(timeslot_minutes=60)

    async def algorithm(self, trigger: Trigger):
        """Algorithm business logic."""
        orderbook = await self.contract_api.get_order_books(
            portfolio_id=self.portfolio_ids, delivery_area=self.delivery_area, delivery_from=trigger.delivery_start, delivery_to=trigger.delivery_end
        )

        self.logger.info(f"Timeslot: {trigger.delivery_start:%Y-%m-%d %H:%M} - {trigger.delivery_end:%Y-%m-%d %H:%M}")
        self.logger.info(f"Triggers: {trigger.all}")
        for contract in orderbook.contracts:
            self.logger.info(
                "CID: %s, rev: %s, start: %s, end: %s", contract.contract_id, contract.revision_no, contract.delivery_start, contract.delivery_end
            )


if __name__ == "__main__":
    TST().start()
