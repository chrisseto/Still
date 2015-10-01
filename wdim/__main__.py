import sys

import wdim.shell
import wdim.server


if __name__ == '__main__':
    if 'shell' in sys.argv:
        return wdim.shell()

    if 'server' in sys.argv:
        return wdim.server()
