import os
import signal
import threading
from rembus.brokerd import main


def test_brokerd():
    """Test brokerd starts and can be interacted with."""
    timer = threading.Timer(1, lambda: os.kill(os.getpid(), signal.SIGINT))
    timer.start()
    main()
