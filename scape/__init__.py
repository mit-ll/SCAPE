# source: http://stackoverflow.com/a/1676069/1869370

from pkgutil import extend_path as _extend_path
__path__ = _extend_path(__path__, __name__)

