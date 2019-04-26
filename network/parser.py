'''
Classes and utilites used in parsing response data from APIs.
'''

import struct

from lzma import LZMADecompressor, LZMAError, FORMAT_AUTO

class FXTickDataParser(object):
    '''
    Parse response data for FX tick data from Dukascopy.
    '''

    def __init__(self):
        '''
        Set metadata for struct unpacking.
        '''

        # Format for struct data.
        # This string is specific to the data types pull from Dukascopy.
        self.struct_format: str = '>3L2f'
        self.row_size: int = struct.calcsize(self.struct_format)

    def _decompress_lzma(self, data: bytes) -> bytes:
        '''
        Correctly decompress LZMA data files.

        NOTE: https://stackoverflow.com/a/37400585/2895581

        :params data: Bytes representation of response data.
        :returns outs: Bytes representation of decompressed LXMA data.
        '''

        results: list = []
        while True:
            decomp = LZMADecompressor(FORMAT_AUTO, None, None)
            try:
                res = decomp.decompress(data)

            # If there is leftover data, then it is not valid LZMA format and
            # we should ignore it.
            # If we encounter an error on the first iteration, bail out.
            except LZMAError:
                if results:
                    break

                else:
                    raise

            results.append(res)
            data = decomp.unused_data
            if not data:
                break

            if not decomp.eof:
                raise LZMAError(f'Compressed data ended before the end-of-stream marker was reached - {self.DUKAS_BASE_URL}')

        return b"".join(results)

    def parse(self, resp: bytes) -> list:
        '''
        Parse raw response into a list ready for processing.

        :params resp: Byte representation of response data. The data is LZMA
                      compressed, so it must be decompressed prior to unpacking
                      data.
        :returns daily_tick_data: List of individual ticks in the market.
        '''

        try:
            data: bytes = self._decompress_lzma(resp)

        except Exception as e:
            raise Exception(str(e) + ' - ' + self.DUKAS_BASE_URL)

        daily_tick_data: list = []
        for chunk_idx in range(0, len(data), self.row_size):
            chunk = struct.unpack(self.struct_format, data[chunk_idx:(chunk_idx + self.row_size)])
            daily_tick_data.append(chunk)

        return daily_tick_data
