# XXX Should we be using this instead?
# 
# from pkgutil import extend_path as _extend_path
# __path__ = _extend_path(__path__, __name__)
#
# source: http://stackoverflow.com/a/1676069/1869370
# 
__import__('pkg_resources').declare_namespace(__name__)
