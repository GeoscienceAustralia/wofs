try:
    from .version import version as __version__
except ImportError:
    __version__ = "Unknown/Not Installed"
