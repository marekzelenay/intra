import asyncio
import functools
import json
import logging
import os
import signal
import ssl
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote
from warnings import warn

import certifi
import ecs_logging
import pytz
import stomper
import websockets
from croniter import CroniterBadCronError, croniter
from powerbot_asyncio_client import (
    AlertApi,
    AlgoHeartbeat,
    AlgoInstanceEvent,
    AlgorithmsApi,
    ApiClient,
    AuctionExchangeApi,
    AuctionOrderApi,
    AuctionsApi,
    AuctionTradeApi,
    AuthenticationApi,
    CapacitiesApi,
    Configuration,
    Contract,
    ContractApi,
    InternalTrade,
    LogsApi,
    MarketApi,
    MessagesApi,
    OrderBookChangedEvent,
    OrderBookChanges,
    OrderBookGroup,
    OrderEntry,
    OrdersApi,
    OwnOrder,
    OwnOrderChanges,
    OwnTradeChanges,
    PortfoliosApi,
    PublicTradeChanges,
    ReportApi,
    Signal,
    SignalChanges,
    SignalsApi,
    SubscriptionsApi,
    TenantsApi,
    Trade,
    TradesApi,
)
from powerbot_asyncio_client.rest import ApiException
from pytz.exceptions import UnknownTimeZoneError


class PrefixAdapter(logging.LoggerAdapter):
    """Logger adapter that prefixes the message with the task name if run inside an event loop."""

    def process(self, msg, kwargs):
        """Process message received by logger."""
        try:
            return f"[{asyncio.current_task().get_name()}] {msg}", kwargs
        except RuntimeError:
            return msg, kwargs


class CancelRun(Exception):
    """Exception to gracefully cancel runs from within sub functions."""

    pass


@dataclass
class Wrapper:
    """Simple dataclass to ease use of swagger client deserializer."""

    data: str


@dataclass
class Trigger:
    """Dataclass to pass triggers to algorithm method."""

    delivery_start: datetime
    delivery_end: datetime

    # noinspection PyTypeChecker
    obce: dict[str, OrderBookChangedEvent] = field(default_factory=dict)  # id:obce
    own_orders: dict[str, OwnOrder] = field(default_factory=dict)  # id:order
    own_trades: dict[str, Trade] = field(default_factory=dict)  # id:trade
    internal_trades: dict[str, InternalTrade] = field(default_factory=dict)  # id:trade
    signals: dict[str, Signal] = field(default_factory=dict)  # id:signal
    orderbooksnapshots: dict[str, OrderBookChanges] = field(default_factory=dict)  # id:orderbooksnapshots
    orderbookgroup: dict[str, OrderBookGroup] = field(default_factory=dict)  # id:orderbookgroup
    contract: dict[str, Contract] = field(default_factory=dict)  # id:contract
    publictrades: dict[str, PublicTradeChanges] = field(default_factory=dict)  # id:publictrades

    @property
    def all(self) -> list:
        """Return all available triggers."""
        return [
            *self.obce.values(),
            *self.own_orders.values(),
            *self.own_trades.values(),
            *self.internal_trades.values(),
            *self.signals.values(),
            *self.orderbooksnapshots.values(),
            *self.orderbookgroup.values(),
            *self.contract.values(),
            *self.publictrades.values(),
        ]


class Algorithm:
    """The Algorithm base class provides a skeleton for our algorithms, as well as some utility functions."""

    portfolio_topics = {"orderbookchangedevent", "ownorders", "owntrades", "signals"}
    delivery_area_topics = {"orderbooksnapshots", "orderbookgroup", "publictrades", "areaorderbookchangedevent"}

    def __init__(self, timeslot_minutes: int = 0, engine: Optional[Any] = None):
        """Constructor.

        Args:
            timeslot_minutes (int): If set, algorithm runs get triggered by the last received OrderBookChangedEvent
                                    for each timeslot of specified length. OBCEs for a queued timeslot get
                                    overwritten and the algorithm is triggered with the most recent one.
                                    If not specified, operate on a per-contract basis.
            engine: Engine, that is used by the current instance.
        """
        if timeslot_minutes < 0 or timeslot_minutes % 5:
            raise ValueError("timeslot_minutes must be a positive multiple of 5")

        self.initialized = False
        self.engine = engine

        self.timeslot = timedelta(minutes=timeslot_minutes)
        self.dt_min = datetime.min.replace(tzinfo=timezone.utc)

        # Get parameters from environment variables
        self.algorithm_id = os.environ["ALGORITHM_ID"]
        self.instance_id = os.environ["INSTANCE_ID"]
        self.api_key = os.environ["API_KEY"]
        self.url_api = os.environ["URL_API"]
        self.url_websocket = os.environ["URL_WEBSOCKET"]
        self.stand_alone = os.environ.get("ALGO_CONTROL") != "1"

        # Init logger
        self.logger = logging.getLogger(f"{self.algorithm_id}-{self.instance_id}")
        ch = logging.StreamHandler()
        log_level = logging.getLevelName(os.environ.get("ALGO_LOG_LEVEL", "").strip())
        self.logger.setLevel(log_level if isinstance(log_level, int) else "INFO")
        ch.setFormatter(
            ecs_logging.StdlibFormatter()
            if os.environ.get("ALGO_LOG_ECS", "").strip() == "1"
            else logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
        )
        self.logger.addHandler(ch)
        self.logger = PrefixAdapter(self.logger, {})
        if not isinstance(log_level, int):
            self.logger.error(f"Invalid log level provided: {log_level}. Defaulting to INFO.")

        if timeslot_minutes:
            self.logger.info(f"Operating in timeslot-mode of {timeslot_minutes} minutes.")
        else:
            self.logger.info("Operating in per-contract-mode.")

        # Set up utility variables
        self.queue = {}
        self.active_tasks: dict
        self.max_concurrent: int
        self.free_workers = 1
        self.run = asyncio.Event()
        self.websocket: websockets.WebSocketClientProtocol
        self.websocket_params: dict = {"ssl": ssl.create_default_context(cafile=certifi.where())} if self.url_websocket.startswith("wss") else {}
        self.ws_retry = 1
        self.exit_code = 1
        self.loop: asyncio.AbstractEventLoop
        self.ob_changed_event_portfolios: Set[str] | None = None

        # Set up remote config variables
        self.heartbeat_seconds: int
        self.portfolio_ids: list[str]
        self.delivery_area: str
        self.exchange: str
        self.config: dict
        self.delivery_areas: Set[str]
        self.cron_schedules: List | None = None

        # Set up swagger client variables
        self.alert_api: AlertApi
        self.api_config: Configuration
        self.api_client: ApiClient
        self.contract_api: ContractApi
        self.orders_api: OrdersApi
        self.market_api: MarketApi
        self.trades_api: TradesApi
        self.algorithms_api: AlgorithmsApi
        self.signals_api: SignalsApi
        self.portfolios_api: PortfoliosApi
        self.auctions_api: AuctionsApi
        self.auction_order_api: AuctionOrderApi
        self.auction_exchange_api: AuctionExchangeApi
        self.auction_trade_api: AuctionTradeApi

    @staticmethod
    def log_own_orders(own_orders: list[OwnOrder]) -> str:
        """Log own orders in a human-readable format.

        Args:
            own_orders (list[OwnOrder]): List of OwnOrder items to be logged.

        Returns:
            str: Human-readable representation of own orders.
        """
        return "\n".join(
            [
                "{:>24} | {:6} | {:4} | {:>6} | {:>7} | {:4} | {:16} | {}".format(
                    o.delivery_area,
                    o.portfolio_id,
                    o.side,
                    round(o.quantity, 1) if o.quantity is not None else "None",
                    round(o.price, 2) if o.price is not None else "None",
                    o.action,
                    "/".join([c.name or c.contract_id for c in o.contracts]),
                    o.txt,
                )
                for o in own_orders
            ]
        )

    @staticmethod
    def log_order_entries(order_entries: list[OrderEntry]) -> str:
        """Log order entries in a human-readable format.

        Args:
            order_entries (list[OrderEntry]): List of OrderEntry items to be logged.

        Returns:
            str: Human-readable representation of order entries.
        """
        return "\n".join(
            [
                "{:>24} | {:6} | {:4} | {:>6} | {:>7} | {:16} | {}".format(
                    o.delivery_area,
                    o.portfolio_id,
                    o.side,
                    round(o.quantity, 1) if o.quantity is not None else "None",
                    round(o.price, 2) if o.price is not None else "None",
                    o.contract_name or o.contract_id,
                    o.txt,
                )
                for o in order_entries
            ]
        )

    def start(self):
        """Start the algorithm."""
        self.logger.warning("Starting new algorithm instance.")

        self.loop = asyncio.get_event_loop()  # noqa
        try:
            try:
                self.loop.run_until_complete(self.loop.create_task(self.__main(), name="Main"))

            except KeyboardInterrupt:
                self.stop(0)

        except BaseException as ex:  # noqa
            if not isinstance(ex, asyncio.CancelledError):
                self.logger.exception(ex)

        finally:
            try:
                self.loop.run_until_complete(self.loop.create_task(self.__teardown(), name="Teardown"))
            except Exception as ex:  # noqa
                self.logger.exception(ex)

            # Close all remaining connections
            self.loop.run_until_complete(self.api_client.rest_client.pool_manager.close())

        self.logger.warning(f"Algorithm instance terminated. {self.exit_code}")
        sys.exit(self.exit_code)

    def stop(self, exit_code: int = 1):
        """Stop the algorithm."""
        self.exit_code = exit_code

        try:
            for task in [task for task in asyncio.all_tasks(self.loop)]:
                task.cancel()
        except RuntimeError:
            # Ignore exception in case the event loop no longer exists
            pass

    def __websocket_reconnect(self):
        """Trigger a websocket reconnect."""
        if self.websocket is not None:
            asyncio.get_running_loop().create_task(self.websocket.close(reason="Mirror switch"))

    async def setup(self):
        """A user-overridable setup function that gets called after the config has been fetched from the server."""
        pass

    async def teardown(self):
        """A user-overridable teardown function that gets called when the algorithm instance is stopped.

        Best practice is to ensure that the setup process has been successful by checking the `self.initialized` variable. If the setup function
        fails, then it might be the case that not all class attributes (e.g. `self.portfolio_ids`) have been initialized.
        """
        pass

    async def __teardown(self):
        """Wrapper function which ensures that the logic of the teardown function will be executed only after all tasks are cancelled.

        Do **NOT** overwrite this function.
        """
        tasks = [task for task in asyncio.all_tasks(self.loop) if task.get_name() != "Teardown"]
        if tasks:
            await asyncio.wait(tasks)
        await self.teardown()

    async def __launch(self):
        """Launch algorithm."""
        # Add signal handlers when not running on windows
        if sys.platform != "win32":
            # Register SIGHUP handler for websocket reconnects
            asyncio.get_running_loop().add_signal_handler(signal.SIGHUP, self.__websocket_reconnect)

            # Stop and exit with code 0 on SIGINT or SIGTERM
            for signame in {"SIGINT", "SIGTERM"}:
                asyncio.get_running_loop().add_signal_handler(getattr(signal, signame), functools.partial(self.stop, 0))

        # Set up swagger API
        self.api_config = Configuration()
        self.api_config.api_key["api_key_security"] = self.api_key
        self.api_config.host = self.url_api
        self.api_config.ssl_ca_cert = certifi.where()
        self.api_client = ApiClient(self.api_config)
        # overwrite datetime deserialize
        self.api_client._ApiClient__deserialize_datetime = datetime.fromisoformat
        self.api_client.user_agent = f"BaseClass/{self.algorithm_id}"
        self.api_client.set_default_header(header_name="Accept-encoding", header_value="gzip,deflate")
        self.alert_api = AlertApi(self.api_client)
        self.algorithms_api = AlgorithmsApi(self.api_client)
        self.authentication_api = AuthenticationApi(self.api_client)
        self.capacities_api = CapacitiesApi(self.api_client)
        self.contract_api = ContractApi(self.api_client)
        self.logs_api = LogsApi(self.api_client)
        self.market_api = MarketApi(self.api_client)
        self.messages_api = MessagesApi(self.api_client)
        self.orders_api = OrdersApi(self.api_client)
        self.portfolios_api = PortfoliosApi(self.api_client)
        self.report_api = ReportApi(self.api_client)
        self.signals_api = SignalsApi(self.api_client)
        self.subscriptions_api = SubscriptionsApi(self.api_client)
        self.tenants_api = TenantsApi(self.api_client)
        self.trades_api = TradesApi(self.api_client)
        self.auctions_api = AuctionsApi(self.api_client)
        self.auction_order_api = AuctionOrderApi(self.api_client)
        self.auction_exchange_api = AuctionExchangeApi(self.api_client)
        self.auction_trades_api = AuctionTradeApi(self.api_client)

        # Attempt to read config from server
        self.logger.debug("Getting config from server.")
        try:
            algo = await self.algorithms_api.get_algorithm(self.algorithm_id)
            config = next(i for i in algo.instances if i.instance_id == self.instance_id)

            if not self.stand_alone and config.status not in ["RUNNING", "FAILED"]:
                self.logger.critical(f"Attempted to start algorithm with target status {config.status}.")
                self.stop(0)
                return

            heartbeat = max(1, (algo.require_heartbeat_every_seconds - 1) / 3)

            self.logger.debug(f"Received config:\n{config}")
            self.exchange = config.exchange

            if "delivery_area" in config.parameters and "delivery_areas" not in config.parameters:
                warn("delivery_area parameter is deprecated, use delivery_areas instead.", FutureWarning, stacklevel=2)
                self.delivery_areas = {config.parameters["delivery_area"]}
                self.delivery_area = config.parameters["delivery_area"]
            elif "delivery_areas" in config.parameters:
                self.delivery_areas = set(config.parameters["delivery_areas"])
            else:
                raise ValueError("Neither delivery_area nor delivery_areas are passed to the configuration!")

            self.portfolio_ids = config.portfolio_ids
            self.heartbeat_seconds = heartbeat
            self.config = config.parameters
            self.delivery_durations = self.config.get("delivery_durations", [])
            self.max_concurrent = min(int(self.config.get("max_concurrent", 1)), 24)  # hard limit 24
            self.topics = set(self.config.get("topics", ""))
            self.pure_autorun = self.config.get("pure_autorun", False)
            schedule_timezone = self.config.get("schedule_timezone", "UTC")
            try:
                self.schedule_timezone = pytz.timezone(schedule_timezone)
            except UnknownTimeZoneError:
                raise ValueError(f"Time zone is unknown: {schedule_timezone}")

            if schedules := self.config.get("schedules"):
                if not isinstance(schedules, list):
                    raise ValueError("Schedules must be a list!")
                try:
                    now = datetime.now(tz=self.schedule_timezone)
                    crons = [croniter(s, start_time=now) for s in schedules]

                    # key is the task, value is the next timestamp
                    self.cron_schedules = {c: max(c.next() - now.timestamp(), 0) for c in crons}

                except CroniterBadCronError:
                    raise ValueError(f"Invalid cron: {schedules}")

            topics = self.topics - self.portfolio_topics - self.delivery_area_topics
            if self.pure_autorun and self.config.get("autorun_seconds", 0) == 0:
                raise ValueError("pure_autorun can be used only with autorun_seconds > 0")
            if self.pure_autorun and self.topics:
                raise ValueError("pure_autorun cannot be combined with subscription topics.")
            if topics:
                raise ValueError(f"Unsupported topics: {topics}")
            if "orderbookgroup" in self.topics and len(self.topics) > 1:
                raise ValueError(
                    "Topic: orderbookgroup cannot be used in combination with other topics due to the unintended simultaneous " "behaviour."
                )
            if "orderbookgroup" in self.topics and self.timeslot:
                raise ValueError("Topic: orderbookgroup cannot be used in combination with 'timeslot_minutes'.")
            if "orderbookgroup" in self.topics and self.config.get("autorun_seconds", 0) > 0:
                raise ValueError("Topic: orderbookgroup cannot be used in combination with autorun.")
            if "orderbookchangedevent" in self.topics and "areaorderbookchangedevent" in self.topics:
                raise ValueError("Topic: orderbookchangedevent cannot be used in combination with areaorderbookchangedevent.")

            # To prevent websocket buffer overflow, when having many (>>10) portfolios
            # The portfolios for the orderbookchangedevent should be chosen manually
            if "orderbookchangedevent" in self.topics:
                # find the portfolios, that satisfy all delivery_areas
                delivery_area_portfolio_mapping: Dict[str, str] = {}

                api_key_details = await self.authentication_api.get_current_api_key_portfolios()

                # select risk management setting of all provided portfolio id's
                portfolios_risk_management_settings = [p.risk_management for p in api_key_details.portfolios if p.id in self.portfolio_ids]

                for portfolio_risk_settings in portfolios_risk_management_settings:
                    # store all delivery_areas portfolio has access to in the set
                    portfolio_delivery_areas = {el.delivery_area for el in portfolio_risk_settings.trading_areas}
                    # update mapping dictionary with the new portfolios
                    delivery_area_portfolio_mapping.update({delivery_area: portfolio_risk_settings.id for delivery_area in portfolio_delivery_areas})

                    if not self.delivery_areas.difference(delivery_area_portfolio_mapping):
                        # if the portfolios that satisfy the provided delivery_areas have been found
                        break

                self.ob_changed_event_portfolios = set(delivery_area_portfolio_mapping.values())

            self.initialized = True
        except Exception as ex:
            if isinstance(ex, ApiException):
                if ex.status == 404:
                    self.logger.error(f"Algorithm {self.algorithm_id} not registered at server.")
                    self.stop(0)
                    return
                else:
                    self.logger.error(f"Server returned HTTP {ex.status}:{ex.body}")
                    return

            elif isinstance(ex, StopIteration):
                self.logger.error(f"Instance {self.instance_id} " f"for algorithm {self.algorithm_id} not registered at server.")
                self.stop(0)
                return

            elif isinstance(ex, ValueError):
                self.logger.error(f"Error parsing config. {ex}")
                return

            else:
                self.logger.exception(f"Error getting config from server:\n{ex}")
                return

        # Call user setup
        self.logger.debug("Running user setup.")
        try:
            await self.setup()

        except Exception as ex:
            self.logger.exception(f"Exception in user setup method:\n{ex}")
            return

        # Schedule websocket handler and task manager
        tasks = [asyncio.create_task(self.__websocket(), name="Websocket"), asyncio.create_task(self.__task_manager(), name="TaskManager")]

        # Schedule optional tasks
        if self.config.get("autorun_seconds", 0) > 0:
            tasks.append(asyncio.create_task(self.__autorun(), name="Autorun"))
        if self.cron_schedules is not None:
            tasks.append(asyncio.create_task(self.__crontask(), name="Crontask"))
        if not self.stand_alone and self.heartbeat_seconds > 0:
            tasks.append(asyncio.create_task(self.__heartbeat(), name="Heartbeat"))

        # Run all tasks until first fails
        done, active = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for d in done:
            try:
                d.result()
            except Exception as ex:
                self.logger.exception(f"Task {d.get_name()} died:\n{ex}")

    async def __main(self):
        """Async main run function."""
        if self.engine:
            await self.engine.launch(algo=self, trigger=Trigger(delivery_start=None, delivery_end=None))
            self.stop(0)

        else:
            await self.__launch()

    async def __task_manager(self):
        """Manage task pool."""
        self.active_tasks = {}
        while True:
            # Wait for a run event
            await self.run.wait()

            # Remove finished tasks from active tasks list and consume result
            if self.active_tasks:
                done, active = await asyncio.wait(self.active_tasks.values(), timeout=0)
                for task in done:
                    del self.active_tasks[task.get_name()]
                    task.result()

            # Collect valid tasks
            self.free_workers = self.max_concurrent - len(self.active_tasks)
            if self.free_workers:
                new = []
                for idx in self.queue:
                    if idx not in self.active_tasks:
                        new.append(idx)
                        self.free_workers -= 1
                        if not self.free_workers:
                            break

                # Feed tasks to workers
                for idx in new:
                    self.active_tasks[idx] = asyncio.create_task(self.__algorithm(self.queue.pop(idx)), name=idx)

            self.run.clear()

    async def __autorun(self):
        """Autorun handler triggers all active contracts or timeslots every n seconds."""
        self.logger.info("Starting autorun daemon.")
        while True:
            self.logger.info("Autorunning.")
            try:
                if self.pure_autorun:
                    await self.algorithm(trigger=None)

                else:
                    for delivery_area in self.delivery_areas:
                        # Get all active contracts
                        obs = await self.contract_api.get_order_books(
                            portfolio_id=[self.portfolio_ids[0]],
                            delivery_area=delivery_area,
                            with_portfolio_information=False,
                            with_risk_settings=False,
                            with_products=False,
                        )
                        self.logger.info(f"Autorun got {len(obs.contracts)} contracts in {delivery_area}.")

                        for contract in obs.contracts:
                            self.__enqueue_trigger(contract, "contract", revision="total_quantity", uid="contract_id", delivery_area=delivery_area)

                        if self.free_workers and obs.contracts:
                            self.run.set()

            except Exception as ex:
                self.logger.error(f"Autorun failed: {ex}", exc_info=True)

            await asyncio.sleep(self.config.get("autorun_seconds"))

    async def __crontask(self):
        """Crontask handler triggers an algorithm run with an empty trigger according to the provided cron."""
        self.logger.info("Starting crontask daemon.")
        while True:
            now = datetime.now(tz=self.schedule_timezone)
            time_to_wait = max(min((c.get_next(start_time=now) - now.timestamp()) for c in self.cron_schedules), 0)
            self.logger.debug(f"Next cron run is in {time_to_wait} seconds.")
            await asyncio.sleep(time_to_wait)
            try:
                await self.algorithm(trigger=None)

            except Exception as ex:
                self.logger.error(f"Crontask failed: {ex}", exc_info=True)

    @staticmethod
    def __is_overlapping(changed_contract: Contract, related_contract: Contract) -> bool:
        """Helper function, that checks whether there is an overlap between changed and related contracts.

        Args:
            changed_contract: Contract that has been changed.
            related_contract: Related to it contract, which was defined as overlapping by the backend.

        Returns:
            True indicates an overlap.
        """
        overlap = max(
            timedelta(hours=0),
            min(changed_contract.delivery_end, related_contract.delivery_end) - max(changed_contract.delivery_start, related_contract.delivery_start),
        )

        return overlap > timedelta(hours=0)

    def __is_delivery_durations_compliant(self, delivery_start: datetime, delivery_end: datetime) -> bool:
        """Helper function, that checks whether the provided delivery period complies with the user-accepted delivery durations.

        Args:
            delivery_start: Delivery start.
            delivery_end: Delivery end.

        Returns:
            True if the provided delivery period is delivery durations compliant.
        """
        delivery_duration = (delivery_end - delivery_start).seconds // 60

        return not self.delivery_durations or delivery_duration in self.delivery_durations

    def __find_overlapping_contracts(self, changed_contract: Contract, related_contracts: List[Contract]) -> Tuple[List[Contract], Contract]:
        """Function that finds overlapping contracts for the changed contract, which have delivery durations as defined by the user.

        If the *delivery_durations* parameter is not then the procedure will be skipped, and the OrderGroup object will be passed to the Trigger
        without any changes.

        Args:
            changed_contract: Contract that has been changed.
            related_contracts: List of related contracts.

        Returns:
            Tuple, where the first element is the list of the related contracts, and the second is the contract with the longest delivery duration.
        """
        if self.delivery_durations:
            # skip contracts, whose delivery durations are not in delivery_durations
            duration_filtered_contracts = [c for c in related_contracts if self.__is_delivery_durations_compliant(c.delivery_start, c.delivery_end)]
            # find all contracts that are overlapping with the changed contract
            changed_contract_overlaps = [c for c in duration_filtered_contracts if self.__is_overlapping(changed_contract, c)]
            # find the longest overlapping contract
            longest_contract = sorted(
                changed_contract_overlaps + [changed_contract], key=lambda x: (x.delivery_end - x.delivery_start), reverse=True
            )[0]
            # filter all contracts that are overlapping with the longest contract
            related_contracts = [c for c in duration_filtered_contracts if self.__is_overlapping(longest_contract, c)]

        else:
            longest_contract = sorted(related_contracts + [changed_contract], key=lambda x: (x.delivery_end - x.delivery_start), reverse=True)[0]

        return related_contracts, longest_contract

    def __enqueue_trigger(
        self, element, store: str = None, revision: str = None, uid: str = None, ordergroup: bool = False, delivery_area: str = None
    ):
        """Sort event into corresponding triggers in queue or create if first."""
        delivery_start = element.changed_contract.delivery_start if ordergroup else element.delivery_start
        delivery_end = element.changed_contract.delivery_end if ordergroup else element.delivery_end

        # skip contract if its delivery duration is not accepted by the user
        if not self.__is_delivery_durations_compliant(delivery_start=delivery_start, delivery_end=delivery_end):
            return

        if ordergroup:
            related_contracts, longest_contract = self.__find_overlapping_contracts(
                changed_contract=element.changed_contract, related_contracts=element.related_contracts
            )
            element.related_contracts = related_contracts

            delivery_start = longest_contract.delivery_start
            delivery_end = longest_contract.delivery_end

            idx = f"{delivery_start} - {delivery_end}"

            trigger = self.queue.setdefault(idx, Trigger(delivery_start, delivery_end))
            if store:
                self.__add_trigger(element, trigger, store, revision, uid, idx, ordergroup, delivery_area)

        # Timeslot mode
        elif self.timeslot:
            # Floor to nearest timeslot
            dt_from = delivery_start - (delivery_start - self.dt_min) % self.timeslot
            # Calculate number of required timeslots
            num = int((delivery_end - delivery_start) / self.timeslot)
            num += 1 if dt_from != delivery_start else 0

            for timeslot in {dt_from + self.timeslot * n for n in range(num)}:
                end = timeslot + self.timeslot
                idx = f"{timeslot} - {end}"
                trigger = self.queue.setdefault(idx, Trigger(timeslot, end))
                if store:
                    self.__add_trigger(element, trigger, store, revision, uid, idx, ordergroup, delivery_area)

        # Contract mode
        else:
            idx = f"{delivery_start} - {delivery_end}"
            trigger = self.queue.setdefault(idx, Trigger(delivery_start, delivery_end))
            if store:
                self.__add_trigger(element, trigger, store, revision, uid, idx, ordergroup, delivery_area)

    def __add_trigger(self, element, trigger: Trigger, store: str, revision: str, uid: str, idx: str, ordergroup: bool, delivery_area: str | None):
        """Add element to trigger if revision is equal or newer."""
        old = getattr(trigger, store).get(idx) if ordergroup else getattr(trigger, store).get(getattr(element, uid))

        old_val = getattr(old, revision, 0)
        old_val = old_val if old_val else 0

        new_val = getattr(element, revision, 0)
        new_val = new_val if new_val else 0

        if old is None or old_val <= new_val:
            if delivery_area is not None:
                store_key = f"{idx}-{delivery_area}" if ordergroup else f"{self.exchange}-{delivery_area}-{getattr(element, uid)}"
            else:
                store_key = getattr(element, uid)

            getattr(trigger, store)[store_key] = element
            if self.free_workers and idx not in self.active_tasks:
                self.run.set()

    async def __heartbeat(self):
        """Heartbeat handler emits a heartbeat every n seconds."""
        self.logger.info("Starting heartbeat daemon.")
        while True:
            self.logger.info("Sending heartbeat.")
            try:
                # noinspection PyUnresolvedReferences
                await self.algorithms_api.submit_heart_beat(
                    algorithm_id=self.algorithm_id, instance_id=self.instance_id, status=AlgoHeartbeat(status="OK", status_text="")
                )
                self.logger.info("Successful heartbeat response is received.")
            except Exception as ex:
                self.logger.error(f"Heartbeat failed: {ex.body if isinstance(ex, ApiException) else ex}")

            await asyncio.sleep(self.heartbeat_seconds)

    async def __websocket(self):
        """Websocket handler listens to incoming messages and feeds them to the trigger queue."""
        self.ws_retry = 1
        while True:
            self.logger.debug("Starting websocket connection.")
            try:
                async with websockets.connect(
                    f"{self.url_websocket}", **self.websocket_params, max_size=None, read_limit=2**25, extra_headers={"api_key": self.api_key}
                ) as self.websocket:
                    await self.websocket.send(stomper.connect(None, None, None))

                    while True:
                        await self.__websocket_message(await self.websocket.recv())

            except Exception as ex:
                await self.websocket.close() if "self.websocket" in globals() else None
                self.logger.warning(f"Websocket connection closed: {ex}")
                await asyncio.sleep(2**self.ws_retry)
                self.ws_retry += 1 if self.ws_retry < 6 else 0

    async def __websocket_message(self, message: str):
        """Handle the received message.

        Args:
            message (str): The received message as a string.
        """
        # Received newline/h are just keep-alive
        if message in ["\n", "h", "h\n"]:
            return

        # Unpack the message from STOMP format
        message = stomper.unpack_frame(message)

        # Once we are connected, subscribe to the channels we want
        if message["cmd"] == "CONNECTED":
            self.logger.info("Connected to websocket. Sending subscription requests...")

            topic = f"/user/{quote(message['headers']['user-name'])}/algoinstancechanges"
            self.logger.info(f"Subscribing to: {topic}")
            await self.websocket.send(stomper.subscribe(topic, topic))

            for topic in self.topics & self.delivery_area_topics:
                for delivery_area in self.delivery_areas:
                    subscription_topic = f"/topic/{topic}-{self.exchange}.{delivery_area}"
                    self.logger.info(f"Subscribing to: {subscription_topic}")
                    await self.websocket.send(stomper.subscribe(subscription_topic, subscription_topic))

            for topic in self.topics & self.portfolio_topics:
                portfolio_ids = self.portfolio_ids
                if topic == "orderbookchangedevent":
                    portfolio_ids = self.ob_changed_event_portfolios

                for portfolio_id in portfolio_ids:
                    subscription_topic = f"/topic/{topic}{'' if topic == 'signals' else f'-{self.exchange}'}.{portfolio_id}"
                    self.logger.info(f"Subscribing to: {subscription_topic}")
                    await self.websocket.send(stomper.subscribe(subscription_topic, subscription_topic))

            self.logger.info("Websocket is up.")
            self.ws_retry = 1

        # Look into the message to see what it is
        elif message["cmd"] == "MESSAGE":
            try:
                # Extract the message class and attempt to deserialize it
                message_class = json.loads(message["body"])["messageClass"].split(".")[3]
                obj = self.api_client.deserialize(Wrapper(message["body"]), message_class)

                if isinstance(obj, OrderBookChangedEvent):
                    if obj.delivery_area in self.delivery_areas:
                        self.__enqueue_trigger(obj, "obce", "revision", "contract_id")

                elif isinstance(obj, OrderBookChanges):
                    self.__enqueue_trigger(obj, "orderbooksnapshots", "revision", "contract_id", delivery_area=obj.delivery_area)

                elif isinstance(obj, OwnOrderChanges):
                    for order in obj.orders:
                        if order.delivery_area in self.delivery_areas:
                            self.__enqueue_trigger(order, "own_orders", "revision_no", "order_id")

                elif isinstance(obj, OwnTradeChanges):
                    if obj.trades:
                        for trade in obj.trades:
                            if trade.delivery_area in self.delivery_areas:
                                self.__enqueue_trigger(trade, "own_trades", "exec_time", "trade_id")

                    if obj.internal_trades:
                        for trade in obj.internal_trades:
                            if self.delivery_areas.intersection({trade.buy_delivery_area, trade.sell_delivery_area}):
                                self.__enqueue_trigger(trade, "internal_trades", "exec_time", "internal_trade_id")

                elif isinstance(obj, SignalChanges):
                    for sig in obj.signals:
                        if sig.delivery_area in self.delivery_areas:
                            self.__enqueue_trigger(sig, "signals", "revision", "id")

                elif isinstance(obj, OrderBookGroup):
                    # delivery area of the orderbookgroup should be taken from the changed contract
                    delivery_area = obj.changed_contract.delivery_area
                    self.__enqueue_trigger(obj, "orderbookgroup", "emitted_at", ordergroup=True, delivery_area=delivery_area)

                elif isinstance(obj, PublicTradeChanges):
                    for trade in obj.trades:
                        if trade.buy_delivery_area in self.delivery_areas or trade.sell_delivery_area in self.delivery_areas:
                            self.__enqueue_trigger(trade, "publictrades", "exec_time", "trade_id")

                elif isinstance(obj, AlgoInstanceEvent):
                    if (
                        not self.stand_alone
                        and obj.algorithm.algo_id == self.algorithm_id
                        and any(i.instance_id == self.instance_id for i in obj.algorithm.instances)
                    ):
                        if obj.change == "STOPPED":
                            self.logger.info("Received STOPPED command.")
                            self.stop(0)

                        else:
                            self.logger.warning(f"Received {obj.change} command, but algo is already running.")

                else:
                    self.logger.warning(f"Unexpected message class received: {message}")

            except Exception as ex:
                self.logger.error(f"Error while processing message: {ex}\n{message}")

        else:
            self.logger.warning(f"Unexpected message received: {message}")

    async def __algorithm(self, trigger: Trigger):
        """Algorithm exception and notification wrapper.

        Args:
            trigger (Trigger): A compound trigger object.
        """
        self.logger.debug("Starting run.")
        try:
            await self.algorithm(trigger)

        except CancelRun as ex:
            self.logger.debug(f"Run canceled: {ex}")

        except Exception as ex:
            self.logger.exception(f"Worker task died:\n{ex}")

        self.run.set()

    async def algorithm(self, trigger: Trigger | None):
        """To be overridden by subclass. Executes algorithm main logic.

        Args:
            trigger (Trigger): A compound trigger object.
        """
        pass
