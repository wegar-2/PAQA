# ----------------------------------------------------------------------------------------------------------------------
# This script contains user-defined errors


class Error(Exception):
    """ This class is the base class for other user-defined errors.  """
    pass


class UnknownStationCodeError(Error):
    """ This error is thrown when an unknown station name appears in the datafiles. """
    pass


class InvalidYearError(Error):
    """ This error is thrown when invalid year is passed to remove_redundant_top_rows_from_sheet
    helper function """
    pass

