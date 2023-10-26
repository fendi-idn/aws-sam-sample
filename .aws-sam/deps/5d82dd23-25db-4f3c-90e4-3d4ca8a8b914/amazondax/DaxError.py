# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from collections import defaultdict

from botocore.exceptions import ClientError as BotoClientError


class DaxErrorCode(object):
    Decoder = 'DecoderException'
    Unrecognized = 'UnrecognizedClientException'
    Authentication = 'MissingAuthenticationTokenException'
    MalformedResult = 'MalformedResultException'
    EndOfStream = 'EndOfStreamException'
    IllegalArgument = 'IllegalArgumentException'
    Validation = 'ValidationException'
    NoRoute = 'NoRouteException'
    ResourceNotFound = 'ResourceNotFoundException'
    ResourceInUse = 'ResourceInUseException'
    ProvisionedThroughputExceeded = 'ProvisionedThroughputExceededException'
    ConditionalCheckFailed = 'ConditionalCheckFailedException'
    InternalServerError = 'InternalServerErrorException'
    ItemCollectionSizeLimitExceeded = 'ItemCollectionSizeLimitExceededException'
    LimitExceeded = 'LimitExceededException'
    Throttling = 'ThrottlingException'
    AccessDenied = 'AccessDeniedException'
    TransactionCanceledException = 'TransactionCanceledException'
    RequestLimitExceededException = 'RequestLimitExceededException'


class DaxClientError(BotoClientError):
    # `retryable` is not used, but removing it causes the integration tests to fail, TODO fix
    def __init__(self, message, ddb_code, retryable=False): # pylint: disable=unused-argument
        error_response, code = _make_error_response(
            message, None, None, ddb_code, None)
        super(DaxClientError, self).__init__(error_response, '')

        self.code = code
        self.codes = None
        self.http_status = None
        self.auth_error = False
        self.wait_for_recovery_before_retry = False


class DaxValidationError(DaxClientError):
    def __init__(self, message):
        super(DaxValidationError, self).__init__(
            message, DaxErrorCode.Validation, False)


class DaxServiceError(BotoClientError):
    def __init__(self, operation_name, message, dax_codes, request_id, ddb_code, http_status, cancellation_reasons=None):
        error_response, code = _make_error_response(
            message, dax_codes, request_id, ddb_code, http_status)
        super(DaxServiceError, self).__init__(error_response, operation_name)

        self.code = code
        self.codes = dax_codes
        self.http_status = http_status
        self.cancellation_reasons = cancellation_reasons
        self.auth_error = _determine_auth_error(self.codes)
        self.wait_for_recovery_before_retry = _determine_wait_for_recovery_before_retry(
            self.codes)

    @classmethod
    def from_no_more_data_exception(cls, operation_name, exc):
        '''Return an exception wrapping a no more data exception.

        Necessary to avoid cluttering up the DaxServiceError constructor, which
        assumes the presence of error codes from DAX.

        '''

        dse = DaxServiceError(
            operation_name, "remote socket is closed", (), None, None, 408)
        dse.__cause__ = exc
        dse.wait_for_recovery_before_retry = True
        return dse


def _make_error_response(message, codes, request_id, ddb_code, http_status):
    # Make an error response that mimics Boto's for compatibility
    def yodict(): return defaultdict(yodict)  # A recursive default dict
    response = yodict()

    code = _pick_error_code(codes) or ddb_code
    if code is not None:
        response['Error']['Code'] = code

    if message:
        response['Error']['Message'] = message

    if request_id:
        response['ResponseMetadata']['RequestId'] = request_id

    if http_status:
        response['ResponseMetadata']['HTTPStatusCode'] = http_status

    # Convert to a regular dict for compatibility
    return _unwrap_defaultdict(response), code


def _unwrap_defaultdict(d):
    if isinstance(d, defaultdict):
        return {k: _unwrap_defaultdict(v) for k, v in d.items()}
    else:
        return d


def _pick_error_code(codes):
    if not codes:
        return None

    if codes == [3, 37, 54]:
        return DaxErrorCode.InternalServerError

    if codes[0] != 4:
        return None

    code = None
    lookup = ERROR_MAP
    for e in codes[1:]:
        lookup = lookup.get(e)
        if lookup is None:
            break
    else:
        code = lookup

    return code


def _determine_auth_error(codes):
    return codes is not None \
        and len(codes) >= 3 \
        and codes[1] == 23 and codes[2] == 31


def _determine_wait_for_recovery_before_retry(codes):
    return len(codes) >= 1 and codes[0] == 2


def is_retryable(exc):
    """
        The Dax client will retry event if it is a retryable exception.
        This function is modeled after the functionality in the Java Client: https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/retry/RetryUtils.java#L77
        Right now, it will retry most error codes.
        Codes arrays whose first value is a "3" are classified as an "Unretryable server error." according to DaxCore.
    """
    if not isinstance(exc, (DaxClientError, DaxServiceError)):
        return False

    return (exc.codes and exc.codes[0] != 3) \
        or exc.code and exc.code in RETRYABLE_ERRORS \
        or exc.http_status in (408, 429, 500, 503)

def is_retryable_with_backoff(exc):
    """
        The Dax client will backoff and jitter a retry event if it is a throttling exception.
        This function is modeled after the functionality in the Java Client: https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/retry/RetryUtils.java#L98
    """
    if not isinstance(exc, (DaxClientError, DaxServiceError)):
        return False

    return exc.code in RETRYABLE_ERRORS_WITH_THROTTLE \
        or exc.http_status == 429


ERROR_MAP = {
    23: {
        24: DaxErrorCode.ResourceNotFound, # [4, 23, 24] - TableNotFoundException - TODO UPDATE
        35: DaxErrorCode.ResourceInUse, # [4, 23, 35] - TableExistsException - TODO UPDATE
    },
    37: {
        38: {
            39: {
                40: DaxErrorCode.ProvisionedThroughputExceeded,
                41: DaxErrorCode.ResourceNotFound,
                43: DaxErrorCode.ConditionalCheckFailed,
                45: DaxErrorCode.ResourceInUse,
                46: DaxErrorCode.Validation,
                47: DaxErrorCode.InternalServerError,
                48: DaxErrorCode.ItemCollectionSizeLimitExceeded,
                49: DaxErrorCode.LimitExceeded,
                50: DaxErrorCode.Throttling,
                58: DaxErrorCode.TransactionCanceledException,
                61: DaxErrorCode.RequestLimitExceededException,
            },
            42: DaxErrorCode.AccessDenied,
            44: (DaxErrorCode.Validation, 'NotImplementedException'),
        }
    }
}

"""
    This set is modeled after the set exposed in the AWS Java SDK:
    https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/retry/RetryUtils.java#L55
"""
RETRYABLE_ERRORS = {
    DaxErrorCode.InternalServerError,
    DaxErrorCode.TransactionCanceledException
}

"""
    This set is modeled after the set exposed in the AWS Java SDK:
    https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/retry/RetryUtils.java#L31
"""
RETRYABLE_ERRORS_WITH_THROTTLE = {
    DaxErrorCode.Throttling,
    DaxErrorCode.ProvisionedThroughputExceeded,
    DaxErrorCode.RequestLimitExceededException,
    DaxErrorCode.LimitExceeded,
}
