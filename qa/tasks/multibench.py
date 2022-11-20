"""
Multibench testing
"""
import contextlib
import logging
import time
import copy
import gevent

from tasks import radosbench

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run multibench

    The config should be as follows:

    multibench:
        time: <seconds to run total>
        segments: <number of concurrent benches>
        radosbench: <config for radosbench>

    example:

    tasks:
    - ceph:
    - multibench:
        clients: [client.0]
        time: 360
    - interactive:
    """
    log.info('Beginning multibench...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    def run_one(num):
        """Run test spawn from gevent"""
        start = time.time()
        benchcontext = (
            copy.copy(config.get('radosbench'))
            if config.get('radosbench')
            else {}
        )

        iterations = 0
        while time.time() - start < int(config.get('time', 600)):
            log.info(f"Starting iteration {iterations} of segment {num}")
            benchcontext['pool'] = f"{str(num)}-{str(iterations)}"
            with radosbench.task(ctx, benchcontext):
                time.sleep()
            iterations += 1

    log.info(f"Starting {str(config.get('segments', 3))} threads")
    segments = [
        gevent.spawn(run_one, i) for i in range(int(config.get('segments', 3)))
    ]


    try:
        yield
    finally:
        [i.get() for i in segments]
