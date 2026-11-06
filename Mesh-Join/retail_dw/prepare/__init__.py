"""Data preparation: cleaning, the Master Data, the online file and the staging table.

Kept free of imports on purpose. The rules module is used at run time by the load
path, and importing it must not pull pandas into a resumed run that never prepares.
"""
