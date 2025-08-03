import logging
import threading
from queue import Queue, Empty


logger = logging.getLogger(__name__)


class BaseWorker:
    """
    Base worker class.
    Each worker must implement the run method to start listening to the queue
    and calling handler functions
    """

    def __init__(self, queue: Queue, tg):
        self.tg = tg
        self._is_enabled = True
        self._queue = queue

    def run(self) -> None:
        raise NotImplementedError()

    def stop(self) -> None:
        raise NotImplementedError()


class SimpleWorker(BaseWorker):
    """Simple one-thread worker"""

    def run(self) -> None:
        self._thread = threading.Thread(target=self._run_thread)
        self._thread.daemon = True
        self._thread.start()

    def _run_thread(self) -> None:
        logger.info("[SimpleWorker] started")

        while True:
            try:
                update = self._queue.get(timeout=0.5)
            except Empty:
                if self._is_enabled:
                    continue
                break

            try:
                update_type: str = update.get('@type', 'unknown')
                if update_type == "error":
                    print("error:", update)
                else:
                    print("update:", update_type)
                for handler in self.tg._update_handlers[update_type]:
                    try:
                        handler(update)
                    except Exception as ex:
                        logger.exception(ex)
            finally:
                self._queue.task_done()

    def stop(self) -> None:
        self._is_enabled = False
        try:
            self._thread.join()
        except RuntimeError as ex:
            logger.exception(ex)
