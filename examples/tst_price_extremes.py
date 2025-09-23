from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Tuple, List

from powerbot_asyncio_client import OrderEntry

from src.algorithm_base_class.algorithm import Algorithm, Trigger


class ExtremePriceAlgo(Algorithm):
    """Algorithm that scans all active contracts, finds the highest and lowest priced ones, and executes orders.

    Price metric:
    - Uses VWAP of public trades over the last hour per contract if available; skips contracts with no recent trades.

    Execution:
    - For the cheapest contract: place BUY.
    - For the most expensive contract: place SELL.
    - Quantity is a small fixed size (default 1.0) for demo purposes.

    Notes:
    - This example runs on any trigger (including autorun/cron) and scans ALL contracts in each configured delivery area.
    - It will skip execution if there are already own orders on the selected contracts to avoid duplicates.
    """

    quantity: float = 1.0

    async def algorithm(self, trigger: Optional[Trigger]):
        # Iterate all configured delivery areas
        for delivery_area in getattr(self, "delivery_areas", set()):
            # Fetch all active contracts for this area
            obs = await self.contract_api.get_order_books(
                portfolio_id=[self.portfolio_ids[0]],
                delivery_area=delivery_area,
                with_portfolio_information=False,
                with_risk_settings=False,
                with_products=False,
            )

            contracts = getattr(obs, "contracts", []) or []
            if not contracts:
                self.logger.info(f"No active contracts found in {delivery_area}.")
                continue

            # Compute VWAP-based price per contract from last hour trades
            priced_contracts: List[Tuple] = []  # (price: float, contract)
            now = datetime.now()
            since = now - timedelta(hours=1)

            for c in contracts:
                try:
                    trades = await self.contract_api.get_public_trades(
                        contract_id=c.contract_id,
                        delivery_area=delivery_area,
                        from_execution_time=since,
                    )
                except Exception as ex:  # network/API issues per contract -> skip
                    self.logger.warning(f"Failed to load trades for {c.contract_id} in {delivery_area}: {ex}")
                    continue

                if not trades:
                    continue

                qty_sum = sum(t.quantity for t in trades)
                if not qty_sum:
                    continue

                vwap = round(sum(t.price * t.quantity for t in trades) / qty_sum, 2)
                priced_contracts.append((vwap, c))

            if len(priced_contracts) < 2:
                self.logger.info(
                    f"Not enough priced contracts (found {len(priced_contracts)}) in {delivery_area} to place both BUY and SELL."
                )
                continue

            # Select extremes
            min_price, min_contract = min(priced_contracts, key=lambda x: x[0])
            max_price, max_contract = max(priced_contracts, key=lambda x: x[0])

            if min_contract.contract_id == max_contract.contract_id:
                self.logger.info("Only one contract with price available, skipping execution.")
                continue

            # Skip if there are already own orders on these two contracts
            try:
                existing_min = await self.orders_api.get_own_orders(
                    contract_id=[min_contract.contract_id],
                    delivery_area=delivery_area,
                    portfolio_id=self.portfolio_ids,
                )
                existing_max = await self.orders_api.get_own_orders(
                    contract_id=[max_contract.contract_id],
                    delivery_area=delivery_area,
                    portfolio_id=self.portfolio_ids,
                )
            except Exception as ex:
                self.logger.warning(f"Failed to load existing orders: {ex}")
                continue

            if existing_min or existing_max:
                self.logger.info("Existing orders found on selected contracts; skipping to avoid duplicates.")
                continue

            # For expected_net_pos, fetch current net positions for these contracts
            async def get_net_pos_map(contract_id: str) -> dict[str, float]:
                try:
                    ob = await self.contract_api.get_order_books(
                        portfolio_id=self.portfolio_ids,
                        contract_id=[contract_id],
                        delivery_area=delivery_area,
                    )
                    c = ob.contracts[0]
                    netpos = {}
                    for pinfo in getattr(c, "portfolio_information", []) or []:
                        netpos[pinfo.portfolio_id] = pinfo.net_pos
                    return netpos
                except Exception:
                    return {}

            min_netpos = await get_net_pos_map(min_contract.contract_id)
            max_netpos = await get_net_pos_map(max_contract.contract_id)

            # Build orders: BUY cheapest at min_price, SELL most expensive at max_price
            orders: List[OrderEntry] = []
            for pf in self.portfolio_ids:
                orders.append(
                    OrderEntry(
                        prod=min_contract.product,
                        contract_id=min_contract.contract_id,
                        portfolio_id=pf,
                        delivery_area=delivery_area,
                        clearing_acct_type="P",
                        ordr_exe_restriction="NON",
                        type="O",
                        validity_res="GFS",
                        state="ACTI",
                        side="BUY",
                        quantity=self.quantity,
                        price=min_price,
                        expected_net_pos=min_netpos.get(pf),
                        txt="tst_price_extremes_buy",
                    )
                )
                orders.append(
                    OrderEntry(
                        prod=max_contract.product,
                        contract_id=max_contract.contract_id,
                        portfolio_id=pf,
                        delivery_area=delivery_area,
                        clearing_acct_type="P",
                        ordr_exe_restriction="NON",
                        type="O",
                        validity_res="GFS",
                        state="ACTI",
                        side="SELL",
                        quantity=self.quantity,
                        price=max_price,
                        expected_net_pos=max_netpos.get(pf),
                        txt="tst_price_extremes_sell",
                    )
                )

            # Place orders
            self.logger.info("Placing orders:\n" + self.log_order_entries(orders))
            try:
                placed = await self.orders_api.add_orders(orders)
            except Exception as ex:
                self.logger.warning(f"Failed to place orders: {ex}")
                continue

            if placed and not any(o for o in placed if getattr(o, "action", None) == "SDEL"):
                self.logger.info("Orders placed:\n" + self.log_own_orders(placed))
            else:
                self.logger.warning("Order placement returned empty or delete actions.")


if __name__ == "__main__":
    ExtremePriceAlgo().start()
