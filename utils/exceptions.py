'''
Custom exception classes.
'''

class FXTickDataRequesterException(Exception):
    pass

class FXTickDataParserException(Exception):
    pass

class FXTickDataProcessorSQLite(Exception):
    pass

class FXTickDataProcessorTabular(Exception):
    pass
