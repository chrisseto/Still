import wdim.server

from tornado.options import options

options.parse_command_line()

wdim.server.serve()
