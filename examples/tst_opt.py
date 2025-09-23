from datetime import datetime, timedelta

from powerbot_asyncio_client import OrderEntry

from src.algorithm_base_class.algorithm import Algorithm, Trigger


class HedgeAlgo(Algorithm):
    """Example of hedging algorithm."""

    async def algorithm(self, trigger: Trigger):
        """Algorithm business logic."""
        # Get the orderbook for the triggering contract
        orderbook = await self.contract_api.get_order_books(
            portfolio_id=self.portfolio_ids,
            delivery_start=trigger.delivery_start,
            delivery_end=trigger.delivery_end,
            delivery_area=self.delivery_area,
        )
        contract = orderbook.contracts[0]

        # Do some debug logging
        self.logger.info(
            "CID/rev: %s/%s | From: %s | To: %s", contract.contract_id, contract.revision_no, trigger.delivery_start, trigger.delivery_end
        )

        # Get own orders
        own_orders = await self.orders_api.get_own_orders(
            contract_id=[contract.contract_id], delivery_area=self.delivery_area, portfolio_id=self.portfolio_ids
        )

        # If there are active orders, abort the current run
        if own_orders:
            return

        # Get signals for the contract
        signals = await self.contract_api.get_contract_signals(
            contract_id=contract.contract_id, delivery_area=self.delivery_area, portfolio_id=self.portfolio_ids
        )

        # Get relevant signals
        signal_pos = next((s for s in signals if s.source == "POSITION"), None)
        signal_price = next((s for s in signals if s.source != "POSITION"), None)

        # Abort run if no position signal is found
        if signal_pos is None or (signal_pos.position_long is None and signal_pos.position_short is None):
            self.logger.info("No position signal.")
            return

        # Calculate dynamic price if signal contains no price
        if not signal_price or "marginal_price" not in signal_price.value:
            self.logger.info("No price signal, falling back to dynamic price calculation.")

            # Calculate the VWAP of public trades in the last hour
            trades = await self.contract_api.get_public_trades(
                contract_id=contract.contract_id, delivery_area=self.delivery_area, from_execution_time=datetime.now() - timedelta(hours=1)
            )
            if trades:
                marg_price = round(sum([i.price * i.quantity for i in trades]) / sum([i.quantity for i in trades]), 2)
            else:
                self.logger.info("No trades found, aborting.")
                return
        else:
            marg