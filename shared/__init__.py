"""Shared pure helpers for quant-portfolio.

Deliberately no imports here: the stock-scoring ``shared`` package re-exports
``data_access`` (which pulls in ``data_manager`` and the scoring stack). The
portfolio repo imports the individual modules it needs instead.
"""
