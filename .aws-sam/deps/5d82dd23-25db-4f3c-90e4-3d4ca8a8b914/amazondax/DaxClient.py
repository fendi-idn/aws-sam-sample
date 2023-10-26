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

from . import AttributeValueEncoder, AttributeValueDecoder, Assemblers, CborTypes, DynamoDbV1Converter
from .Cache import SimpleCache, RefreshingCache
from . import CborDecoder
from .generated import Stubs
from .DaxError import DaxServiceError
from .Constants import DaxMethodIds, DynamoDBOperationNumerals, ReturnValuesOnConditionCheckFailure
from .RequestValidator import RequestValidator
from copy import deepcopy

import logging

logger = logging.getLogger(__name__)


class Request(dict):
    def __init__(self, d=None, **kwargs):
        super(Request, self).__init__()
        if d:
            self.update(d)
        if kwargs:
            self.update(kwargs)
        self.attr_names = None
        self.attr_list_id = None
        self.key_schema = None
        self.key_schema_by_table = None

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value


class DaxClient(object):
    SUCCESS = CborTypes.TYPE_ARRAY + 0  # Success is an empty array

    CACHE_SIZE = 1000
    KEY_CACHE_TTL_MILLIS = 60000

    def __init__(self, tube_pool):
        self._tube_pool = tube_pool

        # Caches are per-node for simplicity
        self._key_cache = RefreshingCache(DaxClient.CACHE_SIZE, self._defineKeySchema, DaxClient.KEY_CACHE_TTL_MILLIS)
        self._attr_list_cache = SimpleCache(DaxClient.CACHE_SIZE, self._defineAttributeList)
        self._attr_list_id_cache = SimpleCache(DaxClient.CACHE_SIZE, self._defineAttributeListId)

    def close(self):
        if self._tube_pool:
            self._tube_pool.close()
            self._tube_pool = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def batch_get_item(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(kwargs, DaxMethodIds.batchGetItem_N697851100_1_Id))
        with self._tube_pool.get() as tube:
            tube.reauth()
            request.key_schema_by_table = {tableName: self._key_cache.get(tableName, tube) \
                                            for tableName in request.RequestItems}

            logger.debug('Sending BatchGetItem request')
            Stubs.write_batchGetItem_N697851100_1(request, tube)

            result = self._decode_result('BatchGetItem', request, Assemblers.batchGetItem_N697851100_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def batch_write_item(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(kwargs, DaxMethodIds.batchWriteItem_116217951_1_Id))
        with self._tube_pool.get() as tube:
            tube.reauth()
            request.key_schema_by_table = {tableName: self._key_cache.get(tableName, tube) \
                                            for tableName in request.RequestItems}

            # Attach AttrListIDs to each PutRequest for later encoding
            for table_name, write_requests in request.RequestItems.items():
                for write_request in write_requests:
                    for write_type, write_info in write_request.items():
                        if write_type == 'PutRequest':
                            attr_names = AttributeValueEncoder._get_canonical_attribute_list(
                                write_info['Item'],
                                request.key_schema_by_table[table_name])
                            write_info['attr_names'] = attr_names
                            write_info['attr_list_id'] = self._attr_list_id_cache.get(attr_names, tube)

            logger.debug('Sending BatchWriteItem request')
            Stubs.write_batchWriteItem_116217951_1(request, tube)

            result = self._decode_result('BatchWriteItem', request, Assemblers.batchWriteItem_116217951_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def get_item(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(kwargs, DaxMethodIds.getItem_263244906_1_Id))
        with self._tube_pool.get() as tube:
            tube.reauth()
            request.key_schema = self._key_cache.get(request.TableName, tube)

            logger.debug('Sending GetItem request')
            Stubs.write_getItem_263244906_1(request, tube)

            result = self._decode_result('GetItem', request, Assemblers.getItem_263244906_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def put_item(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(kwargs, DaxMethodIds.putItem_N2106490455_1_Id))
        with self._tube_pool.get() as tube:
            tube.reauth()
            request.key_schema = self._key_cache.get(request.TableName, tube)
            request.attr_names = AttributeValueEncoder._get_canonical_attribute_list(request.Item, request.key_schema)
            request.attr_list_id = self._attr_list_id_cache.get(request.attr_names, tube)

            logger.debug('Sending PutItem request')
            Stubs.write_putItem_N2106490455_1(request, tube)

            result = self._decode_result('PutItem', request, Assemblers.putItem_N2106490455_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def update_item(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(kwargs, DaxMethodIds.updateItem_1425579023_1_Id))
        with self._tube_pool.get() as tube:
            tube.reauth()
            request.key_schema = self._key_cache.get(request.TableName, tube)

            logger.debug('Sending UpdateItem request')
            Stubs.write_updateItem_1425579023_1(request, tube)

            result = self._decode_result('UpdateItem', request, Assemblers.updateItem_1425579023_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def delete_item(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(kwargs, DaxMethodIds.deleteItem_1013539361_1_Id))
        with self._tube_pool.get() as tube:
            tube.reauth()
            request.key_schema = self._key_cache.get(request.TableName, tube)

            logger.debug('Sending DeleteItem request')
            Stubs.write_deleteItem_1013539361_1(request, tube)

            result = self._decode_result('DeleteItem', request, Assemblers.deleteItem_1013539361_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def query(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(kwargs, DaxMethodIds.query_N931250863_1_Id))
        with self._tube_pool.get() as tube:
            tube.reauth()
            request.key_schema = self._key_cache.get(request.TableName, tube)

            logger.debug('Sending Query request')
            Stubs.write_query_N931250863_1(request, tube)

            result = self._decode_result('Query', request, Assemblers.query_N931250863_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def scan(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(kwargs, DaxMethodIds.scan_N1875390620_1_Id))
        with self._tube_pool.get() as tube:
            tube.reauth()
            request.key_schema = self._key_cache.get(request.TableName, tube)

            logger.debug('Sending Scan request')
            Stubs.write_scan_N1875390620_1(request, tube)

            result = self._decode_result('Scan', request, Assemblers.scan_N1875390620_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def _convert_to_transaction_get_items_format(self, request, tube):
        """
        Segregate request data into separate arrays which will further be used for encoding the request. At this point basic validation from
        SDK is already done.
        :param request:
        :param tube:
        :return:
        """
        table_names = []
        keys = []
        projection_expressions = []

        key_set_per_table = {}
        key_schema_per_table = {}

        for transact_get_item in request['TransactItems']:

            operation_detail = list(transact_get_item.items())[0][1]

            table_name = operation_detail['TableName']
            key = operation_detail['Key']
            projection_expression = operation_detail.get('ProjectionExpression')
            attr_names = operation_detail.get('ExpressionAttributeNames')

            RequestValidator.validate_expression(proj_expr=projection_expression, attr_names=attr_names)

            key_schema = self._key_cache.get(table_name, tube)
            key_schema_per_table[table_name] = key_schema

            RequestValidator.validate_key(key, key_schema)
            key_bytes = AttributeValueEncoder.encode_key(key, key_schema)
            key_set = key_set_per_table.get(table_name)
            if not key_set:
                key_set = set()
                key_set_per_table[table_name] = key_set
            if key_bytes in key_set:
                raise RequestValidator.new_validation_exception("Transaction request cannot include multiple operations on one item")
            key_set.add(key_bytes)

            table_names.append(table_name.encode('utf-8'))
            keys.append(key_bytes)
            if projection_expression:
                encoded_expr = AttributeValueEncoder._parse_expressions_only(operation_detail)
                operation_detail['projection_ordinals'] = AttributeValueEncoder._prepare_projection(projection_expression,
                                                                                                    encoded_expr.ExpressionAttributeNames)
                projection_expression = encoded_expr.Projection
            projection_expressions.append(projection_expression)

        request.key_schema_by_table = key_schema_per_table
        request['tableNames'] = table_names
        request['keys'] = keys
        request['projectionExpressions'] = projection_expressions
        return request

    def transact_get_items(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(deepcopy(kwargs), DaxMethodIds.transactGetItems_1866287579_1_Id))

        with self._tube_pool.get() as tube:
            tube.reauth()

            request = self._convert_to_transaction_get_items_format(request, tube)

            logger.debug('Sending transact get items request')
            Stubs.write_transactGetItems_1866287579_1(request, tube)

            result = self._decode_result('TransactGetItems', request, Assemblers.transactGetItems_1866287579_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def _convert_to_transaction_write_items_format(self, request, tube):
        """
        Segregate request data into separate arrays which will further be used for encoding the request. At this point basic validation from
        SDK is already done.
        :param request:
        :param tube:
        :return:
        """
        operations = []
        table_names = []
        keys = []
        values = []
        return_values = None  # currently not used but expected by decoder on server side.
        return_values_on_condition_check_failures = []
        condition_expressions = []
        update_expressions = []

        key_set_per_table = {}
        key_schema_per_table = {}

        for transact_write_item in request['TransactItems']:
            key = None
            item = None

            operation_count = len(transact_write_item)
            if operation_count > 1:
                raise RequestValidator.new_validation_exception(
                    'TransactItems can only contain one of ConditionCheck, Put, Update or Delete')
            if operation_count == 0:
                raise RequestValidator.new_validation_exception(
                    "Invalid Request: TransactWriteRequest should contain Delete or Put or Update request")

            operation, operation_detail = list(transact_write_item.items())[0]
            if operation not in ['ConditionCheck', 'Put', 'Delete', 'Update']:
                raise RequestValidator.new_validation_exception(
                    "Invalid Request: TransactWriteRequest should contain Delete or Put or Update request")

            table_name = operation_detail['TableName']
            condition_expression = operation_detail.get('ConditionExpression')
            update_expression = operation_detail.get('UpdateExpression')
            attr_names = operation_detail.get('ExpressionAttributeNames')
            attr_vals = operation_detail.get('ExpressionAttributeValues')

            if operation == "ConditionCheck":
                operation = DynamoDBOperationNumerals.CHECK
                key = operation_detail['Key']
            if operation == "Put":
                operation_detail = transact_write_item["Put"]
                operation = DynamoDBOperationNumerals.PUT
                item = operation_detail['Item']
            if operation == "Delete":
                operation = DynamoDBOperationNumerals.DELETE
                key = operation_detail['Key']
            if operation == "Update":
                operation = DynamoDBOperationNumerals.PARTIAL_UPDATE
                key = operation_detail['Key']

            RequestValidator.validate_expression(cond_expr=condition_expression, upd_expr=update_expression, attr_names=attr_names,
                                                 attr_vals=attr_vals)
            # validate and encode key / value
            key_schema = self._key_cache.get(table_name, tube)
            key_schema_per_table[table_name] = key_schema
            if key:
                RequestValidator.validate_key(key, key_schema)
            key_bytes = AttributeValueEncoder.encode_key(key if key else item, key_schema)
            key_set = key_set_per_table.get(table_name)
            if not key_set:
                key_set = set()
                key_set_per_table[table_name] = key_set
            if key_bytes in key_set:
                raise RequestValidator.new_validation_exception("Transaction request cannot include multiple operations on one item")
            key_set.add(key_bytes)
            #  write to final keys array
            keys.append(key_bytes)

            if item:
                attr_names = AttributeValueEncoder._get_canonical_attribute_list(item, key_schema)
                attrListId = self._attr_list_id_cache.get(attr_names, tube)
                values.append(AttributeValueEncoder.encode_values(item, key_schema, attr_names, attrListId))
            else:
                values.append(None)

            operations.append(operation)
            table_names.append(table_name.encode('utf-8'))
            return_values_on_condition_check_failure = ReturnValuesOnConditionCheckFailure.NONE
            if operation_detail.get('ReturnValuesOnConditionCheckFailure') == 'ALL_OLD':
                return_values_on_condition_check_failure = ReturnValuesOnConditionCheckFailure.ALL_OLD
            return_values_on_condition_check_failures.append(return_values_on_condition_check_failure)

            # encode expressions along with attribute name and values
            encoded_expr = AttributeValueEncoder._parse_expressions_only(operation_detail)
            if encoded_expr.Condition:
                condition_expressions.append(encoded_expr.Condition)
            else:
                condition_expressions.append(None)
            if encoded_expr.Update:
                update_expressions.append(encoded_expr.Update)
            else:
                update_expressions.append(None)

        request.key_schema_by_table = key_schema_per_table
        request['operations'] = operations
        request['tableNames'] = table_names
        request['keys'] = keys
        request['values'] = values
        request['returnValues'] = return_values
        request['returnValuesOnConditionCheckFailure'] = return_values_on_condition_check_failures
        request['conditionExpressions'] = condition_expressions
        request['updateExpressions'] = update_expressions
        return request

    def transact_write_items(self, **kwargs):
        request = Request(DynamoDbV1Converter.convert_request(deepcopy(kwargs), DaxMethodIds.transactWriteItems_N1160037738_1_Id))

        with self._tube_pool.get() as tube:
            tube.reauth()

            request = self._convert_to_transaction_write_items_format(request, tube)

            logger.debug('Sending transact write items request')
            Stubs.write_transactWriteItems_N1160037738_1(request, tube)

            result = self._decode_result('TransactWriteItems', request, Assemblers.transactWriteItems_N1160037738_1, tube)
            result = self._resolve_attribute_values(result, tube)
            return result

    def endpoints(self):
        with self._tube_pool.get() as tube:
            tube.reauth()

            logger.debug('Sending endpoints request')
            Stubs.write_endpoints_455855874_1(tube)

            return self._decode_result('endpoints', None, Assemblers.endpoints_455855874_1, tube)

    def _defineKeySchema(self, table_name, tube):
        logger.debug('Sending defineKeySchema request')
        Stubs.write_defineKeySchema_N742646399_1(table_name, tube)

        return self._decode_result('defineKeySchema', None, Assemblers.defineKeySchema_N742646399_1, tube)

    def _defineAttributeList(self, attr_list_id, tube):
        if attr_list_id == 1:
            # 1 is defined to be an empty array
            return []

        logger.debug('Sending defineAttributeList request')
        Stubs.write_defineAttributeList_670678385_1(attr_list_id, tube)

        return self._decode_result('defineAttributeList', None, Assemblers.defineAttributeList_670678385_1, tube)

    def _defineAttributeListId(self, attr_list, tube):
        if len(attr_list) == 0:
            # An empty list is defined to be 1
            return 1

        logger.debug('Sending defineAttributeListId request')
        Stubs.write_defineAttributeListId_N1230579644_1(attr_list, tube)

        return self._decode_result('defineAttributeListId', None, Assemblers.defineAttributeListId_N1230579644_1, tube)

    def _decode_result(self, operation_name, request, assembler, tube):
        logger.debug('Decoding %s response', operation_name)
        try:
            status = tube.peek()
        except CborDecoder.NoMoreData as e:
            raise DaxServiceError.from_no_more_data_exception(operation_name, e)

        if status == DaxClient.SUCCESS:
            tube.skip()  # Throw away the empty error header
            return assembler(request, tube)
        else:
            return self._handle_error(operation_name, tube)

    def _handle_error(self, operation_name, tube):
        codes = tube.read_object()
        message = tube.read_string()
        exc_info = tube.read_object() or (None, None, None)
        raise DaxServiceError(operation_name, message, codes, *exc_info)

    def _resolve_attribute_values(self, result, tube):
        if 'Item' in result:
            self._resolve_item_attribute_values(result['Item'], tube)

        if 'Attributes' in result:
            self._resolve_item_attribute_values(result['Attributes'], tube)

        if 'Items' in result:
            for item in result['Items']:
                self._resolve_item_attribute_values(item, tube)

        if 'Responses' in result:
            if isinstance(result['Responses'], list):
                # transact-get-items
                for response in result['Responses']:
                    # skip the empty responses
                    if response:
                        self._resolve_item_attribute_values(response['Item'], tube)
            else:
                # batch-get-items
                for table_results in result['Responses'].values():
                    for item in table_results:
                        self._resolve_item_attribute_values(item, tube)

        if 'UnprocessedItems' in result:
            for unprocessed_requests in result['UnprocessedItems'].values():
                for unprocessed_request in unprocessed_requests:
                    for write_type, write_request in unprocessed_request.items():
                        if write_type == 'PutRequest':
                            self._resolve_item_attribute_values(write_request['Item'], tube)

        return result

    def _resolve_item_attribute_values(self, item, tube):
        if '_attr_list_id' in item:
            attr_names = self._attr_list_cache.get(item['_attr_list_id'], tube)
            AttributeValueDecoder.deanonymize_attribute_values(item, attr_names)
