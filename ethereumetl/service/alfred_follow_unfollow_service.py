# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import logging
from builtins import map
import time
from ethereumetl.domain.alfred_follow_unfollow import AlfredFollowUnfollowLog
from ethereumetl.utils import chunk_string, hex_to_dec, to_normalized_address

# https://ethereum.stackexchange.com/questions/12553/understanding-logs-and-log-blooms
TOPICS_TO_LISTEN = [
    "0xe06da1cbe3a0188be1a0c74902a78f6ed562c67538f262251470773cc1cdc0cb",
    "0x1e60c5d290a79aa84ee8cafce921f82623e68027f2d91106c766788b797e0520",
    "0x7fb1c74d1ea6aa1c9c585e17ce8274c8ff98745e85e7459b73f87d784494f58e",
    "0x9a382661d6573da86db000471303be6f0b2b1bb66089b08e3c16a85d7b6e94f8",
    "0x21435c5b618d77ff3657140cd3318e2cffaebc5e0e1b7318f56a9ba4044c3ed2",
    "0x1be316b94d38c07bd41cdb4913772d0a0a82802786a2f8b657b6e85dbcdfc641",
]
logger = logging.getLogger(__name__)


class AlfredFollowLogsExtractor(object):
    def extract_transfer_from_log(self, receipt_log):
        topics = receipt_log.topics
        topics_with_data = topics + split_to_words(receipt_log.data)
        if len(topics) < 1:
            return None
        if ((topics[0]).casefold()) in TOPICS_TO_LISTEN:
            filtered_logs = AlfredFollowUnfollowLog()
            filtered_logs.address = to_normalized_address(receipt_log.address)
            filtered_logs.topics = receipt_log.topics
            filtered_logs.data = receipt_log.data
            filtered_logs.transaction_hash = receipt_log.transaction_hash
            filtered_logs.log_index = receipt_log.log_index
            filtered_logs.block_number = receipt_log.block_number
            filtered_logs.to_address = receipt_log.to_address
            filtered_logs.from_address = receipt_log.from_address
            filtered_logs.transaction_index = receipt_log.transaction_index
            return filtered_logs
        return None


def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = list(map(lambda word: "0x" + word, words))
        return words_with_0x
    return []


def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address("0x" + param[-40:])
    else:
        return to_normalized_address(param)
