from datetime import datetime, timedelta
from random import randint

from powerbot_asyncio_client import OrderEntry

from src.algorithm_base_class.algorithm import Algorithm, Trigger


class FlexAlgo(Algorithm):
    """Example of flexibility trading algorithm."""

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
                marg_price = randint(30, 50)
        else:
            marg_price = signal_price.value["marginal_price"]

        # Place an order based on net position
        # Get net position
        net_position = next(
            portfolio_info.net_pos for portfolio_info in contract.portfolio_information if portfolio_info.portfolio_id in self.portfolio_ids
        )

        # Calculate open position
        open_position = ((signal_pos.position_long or 0) - (signal_pos.position_short or 0)) * -1 - net_position

        # If the open position is 0, try to get the net position to 0 again
        open_position = open_position if open_position else (net_position * -1)

        # Calculate an appropriate price
        price = marg_price - 2 if open_position > 0 else marg_price + 2

        orders = []

        # Create order
        for portfolio in self.portfolio_ids:
            orders.append(
                OrderEntry(
                    prod=contract.product,
                    contract_id=contract.contract_id,
                    portfolio_id=portfolio,
                    delivery_area=self.delivery_area,
                    clearing_acct_type="P",
                    ordr_exe_restriction="NON",
                    type="O",
                    validity_res="GFS",
                    state="ACTI",
                    side="BUY" if open_position > 0 else "SELL",
                    quantity=abs(open_position),
                    price=price,
                    expected_net_pos=net_position,
                    txt="tst_flex_algo",
                )
            )

        # Send order to market
        self.log_order_entries(orders)
        placed_orders = await self.orders_api.add_orders(orders)

        # Check if order has been placed
        if placed_orders and not any(o for o in placed_orders if o.action == "SDEL"):
            self.log_own_orders(placed_orders)
        else:
            self.logger.warning("Failed to place orders.")


if __name__ == "__main__":
    FlexAlgo().start()
