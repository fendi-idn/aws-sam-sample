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

from .CborSExprGenerator import CborSExprGenerator
from .DaxError import DaxValidationError
from botocore.validate import ParamValidator
from six import iteritems
import re

MAX_EXPRESSION_SIZE = 4096
MAX_PARAMETER_MAP_KEY_SIZE = 255
MAX_PARAMETER_MAP_ENTRIES = 2097152
MAX_ATTRIBUTE_NAME_SIZE = 65535


class RequestValidator:
    _PARAM_VALIDATOR = ParamValidator()

    @staticmethod
    def validate_key(item, key_schema):
        if not item:
            raise RequestValidator.new_validation_exception(
                'Value null at %s failed to satisfy constraint: Member must not be null' % item)

        if not len(item.keys()) is len(key_schema):
            raise RequestValidator.new_validation_exception('The number of conditions on the keys is invalid')

    @staticmethod
    def validate_expr_attr_names(attr_names):
        attr_name_map_size = 0

        if len(attr_names.keys()) == 0:
            raise RequestValidator.new_validation_exception('ExpressionAttributeNames must not be empty')
        elif len(attr_names.keys()) >= MAX_PARAMETER_MAP_ENTRIES:
            raise RequestValidator.new_validation_exception('ExpressionAttributeNames exceeds max size')
        else:
            for k, v in attr_names.items():
                if k is None:
                    raise RequestValidator.new_validation_exception('ExpressionAttributeNames contains invalid key: null')

                attr_name_map_size += len(k)
                if v is None:
                    attr_name_map_size += 0
                elif len(v) == 0:
                    raise RequestValidator.new_validation_exception('ExpressionAttributeNames contains invalid value: for key %s' % k)
                elif len(v) > MAX_ATTRIBUTE_NAME_SIZE:
                    raise RequestValidator.new_validation_exception('Member must have length less than or equal to %d, Member must have '
                                                                    'length greater than or equal to 0' % MAX_ATTRIBUTE_NAME_SIZE)
                else:
                    attr_name_map_size += len(v)

                if len(k) == 0:
                    raise RequestValidator.new_validation_exception(
                        'ExpressionAttributeNames contains invalid key: The expression attribute map contains an empty key')
                elif k[0] != CborSExprGenerator.ATTRIBUTE_NAME_PREFIX:
                    raise RequestValidator.new_validation_exception('Syntax error, ExpressionAttributeNames contains invalid key: %s' % k)
                elif len(k) > MAX_PARAMETER_MAP_KEY_SIZE:
                    raise RequestValidator.new_validation_exception(
                        'ExpressionAttributeNames contains invalid key: The expression attribute map contains a key that is too long')
                elif not v:
                    raise RequestValidator.new_validation_exception('ExpressionAttributeNames must not be empty')
        return attr_name_map_size

    @staticmethod
    def validate_expr_attr_values(attr_vals):
        attr_values_map_size = 0
        if len(attr_vals.keys()) == 0:
            raise RequestValidator.new_validation_exception('ExpressionAttributeValues must not be empty')
        elif len(attr_vals.keys()) >= MAX_PARAMETER_MAP_ENTRIES:
            raise RequestValidator.new_validation_exception('ExpressionAttributeValues exceeds max size')
        else:
            for k, v in attr_vals.items():
                if k == None:
                    raise RequestValidator.new_validation_exception('ExpressionAttributeValues contains invalid key: null')
                attr_values_map_size += len(k)
                attr_values_map_size += RequestValidator.simple_attr_val_length(v)
                if len(k) == 0:
                    raise RequestValidator.new_validation_exception(
                        'ExpressionAttributeValues contains invalid key: The expression attribute map contains an empty key')
                elif k[0] != CborSExprGenerator.ATTRIBUTE_VALUE_PREFIX:
                    raise RequestValidator.new_validation_exception('Syntax error, ExpressionAttributeValues contains invalid key: %s' % k)
                elif len(k) > MAX_PARAMETER_MAP_KEY_SIZE:
                    raise RequestValidator.new_validation_exception(
                        'ExpressionAttributeValues contains invalid key: The expression attribute map contains a key that is too long')
                RequestValidator.validate_attribute_value(v)
            return attr_values_map_size

    @staticmethod
    def simple_attr_val_length(v):
        if v is None:
            return 0
        if v.get('S'):
            return len(v['S'])
        if v.get('B'):
            return len(v['B'])
        if v.get('N'):
            return len(v['N'])
        if v.get('BS'):
            size = 0
            for b in v['BS']:
                size += len(b)
            return size
            # Only the primitive types are expected
        return 0

    @staticmethod
    def validate_attribute_value(attr):
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-attributes
        if attr is None:
            return
        if 'SS' in attr.keys() and len(attr['SS']) == 0:
            raise RequestValidator.new_validation_exception(
                'One or more parameter values were invalid: An string set may not be empty')
        elif attr.get('SS'):  
            for s in attr['SS']:
                if s is None:
                    raise RequestValidator.new_validation_exception(
                        'One or more parameter values were invalid: An string set may not have a null string as a member')
        elif 'BS' in attr.keys() and len(attr['BS']) == 0:
            raise RequestValidator.new_validation_exception(
                'One or more parameter values were invalid: Binary sets should not be empty')
        elif 'NS' in attr.keys() and len(attr['NS']) == 0:
            raise RequestValidator.new_validation_exception(
                'One or more parameter values were invalid: An number set  may not be empty')
        elif attr.get('M'):
            RequestValidator.validate_item(attr['M'])
        elif attr.get('L'):
            for av in attr['L']:
                RequestValidator.validate_attribute_value(av)

    @staticmethod
    def validate_item(item):
        for k, v in item.items():
            if k is None:
                if v is None:
                    # For some reason DDB SDK accept (null: null) but deny (null: av). (null: null) doesn't represent anything in actual item.
                    raise RequestValidator.new_validation_exception(
                        "Unable to marshall request to JSON: Unable to marshall request to JSON: Unable to marshall request to JSON: Unable to marshall request to JSON")
                else:
                    item.pop(k)
            RequestValidator.validate_attribute_value(v)

    @staticmethod
    def validate_expression(cond_expr=None, upd_expr=None, proj_expr=None, filter_expr=None, key_cond_expr=None, cond_op=None,
                            exp_attr_vals=None, attr_updates=None, attributes_to_get=None, query_filter=None, scan_filter=None,
                            key_condition=None, attr_names=None, attr_vals=None):
        attr_name_map_size = 0
        if attr_names:
            if not cond_expr and not upd_expr and not proj_expr and not filter_expr and not key_cond_expr:
                raise RequestValidator.new_validation_exception('ExpressionAttributeNames can only be specified when using expressions')
            attr_name_map_size = RequestValidator.validate_expr_attr_names(attr_names)
            if attr_name_map_size > MAX_PARAMETER_MAP_ENTRIES:
                raise RequestValidator.new_validation_exception('ExpressionAttributeNames exceeds max size')

        attr_values_map_size = 0
        if attr_vals:
            if not cond_expr and not upd_expr and not filter_expr and not key_cond_expr:
                RequestValidator.new_validation_exception('ExpressionAttributeValues can only be specified when using expressions')
            attr_values_map_size = RequestValidator.validate_expr_attr_values(attr_vals)
            if attr_values_map_size > MAX_PARAMETER_MAP_ENTRIES:
                raise RequestValidator.new_validation_exception('ExpressionAttributeValues exceeds max size')

        if (attr_name_map_size + attr_values_map_size) > MAX_PARAMETER_MAP_ENTRIES:
            raise RequestValidator.new_validation_exception(
                'Combined size of ExpressionAttributeNames and ExpressionAttributeValues exceeds max size')

        if exp_attr_vals is not None:
            for k, v in exp_attr_vals.items():
                if v and v['Value'] and v['AttributeValueList']:
                    raise RequestValidator.new_validation_exception('One or more parameter values were invalid: Value and '
                                                                    'AttributeValueList cannot be used together for Attribute: %s' % k)

        if cond_expr or upd_expr:
            if attr_updates or cond_op or exp_attr_vals:
                raise RequestValidator.new_validation_exception(
                    'Can not use both expression and non-expression parameters in the same request')
            if cond_expr is not None:
                if len(cond_expr) == 0:
                    raise RequestValidator.new_validation_exception('Invalid ConditionExpression: The expression can not be empty')
                elif len(cond_expr) > MAX_EXPRESSION_SIZE:
                    RequestValidator.new_validation_exception(
                        'Invalid ConditionExpression: Expression size has exceeded the maximum allowed size (%d)' % MAX_EXPRESSION_SIZE)
            if upd_expr is not None:
                if len(upd_expr) == 0:
                    RequestValidator.new_validation_exception('Invalid UpdateExpression: The expression can not be empty')
                elif len(upd_expr) > MAX_EXPRESSION_SIZE:
                    RequestValidator.new_validation_exception(
                        'Invalid UpdateExpression: Expression size has exceeded the maximum allowed size (%d)' % MAX_EXPRESSION_SIZE)

        if key_cond_expr is not None:
            if len(key_cond_expr) == 0:
                raise RequestValidator.new_validation_exception('Invalid KeyConditionExpression: The expression can not be empty')
            elif len(key_cond_expr) > MAX_EXPRESSION_SIZE:
                raise RequestValidator.new_validation_exception(
                    'Invalid KeyConditionExpression: Expression size has exceeded the maximum allowed size (%d)' % MAX_EXPRESSION_SIZE)

        if proj_expr is not None:
            if len(proj_expr) == 0:
                raise RequestValidator.new_validation_exception('Invalid ProjectionExpression: The expression can not be empty')
            elif RequestValidator.expression_length(proj_expr, attr_names) > MAX_EXPRESSION_SIZE:
                raise RequestValidator.new_validation_exception(
                    'Invalid ProjectionExpression: Expression size has exceeded the maximum allowed size (%d)' % MAX_EXPRESSION_SIZE)

        if filter_expr is not None:
            if len(filter_expr) == 0:
                raise RequestValidator.new_validation_exception('Invalid FilterExpression: The expression can not be empty')
            elif len(filter_expr) > MAX_EXPRESSION_SIZE:
                raise RequestValidator.new_validation_exception(
                    'Invalid FilterExpression: Expression size has exceeded the maximum allowed size (%d)' % MAX_EXPRESSION_SIZE)

        if proj_expr or filter_expr or key_cond_expr:
            non_expr_params = ''
            expr_params = ''
            # This is required by some of the tests. Probably over-matching but whatever.
            # The order is important. don't re-arrange unless necessary
            if proj_expr:
                expr_params = RequestValidator.append_parameter_name(expr_params, 'ProjectionExpression')
            if filter_expr:
                expr_params = RequestValidator.append_parameter_name(expr_params, 'FilterExpression')
            if key_cond_expr:
                expr_params = RequestValidator.append_parameter_name(expr_params, 'KeyConditionExpression')
            if attributes_to_get:
                non_expr_params = RequestValidator.append_parameter_name(non_expr_params, 'AttributesToGet')
            if scan_filter:
                non_expr_params = RequestValidator.append_parameter_name(non_expr_params, 'ScanFilter')
            if query_filter:
                non_expr_params = RequestValidator.append_parameter_name(non_expr_params, 'QueryFilter')
            if cond_op:
                non_expr_params = RequestValidator.append_parameter_name(non_expr_params, 'ConditionalOperator')
            if key_cond_expr and key_condition:
                non_expr_params = RequestValidator.append_parameter_name(non_expr_params, 'KeyConditions')
            if non_expr_params:
                raise RequestValidator.new_validation_exception(
                    'Can not use both expression and non-expression parameters in the same request: Non-expression parameters: '
                    '{} Expression parameters: {}'.format(non_expr_params, expr_params))

        if cond_op:
            if not exp_attr_vals and not query_filter and not scan_filter:
                raise RequestValidator.new_validation_exception('ConditionalOperator cannot be used without Filter or Expected')
            if (exp_attr_vals and len(exp_attr_vals.keys()) <= 1) or ((query_filter is not None and len(query_filter.keys()) <= 1) or (
                    scan_filter is not None and len(scan_filter.keys()) <= 1)):
                raise RequestValidator.new_validation_exception(
                    'ConditionalOperator can only be used when Filter or Expected has two or more elements')

    @staticmethod
    def expression_length(expr, subs):
        if not expr:
            return 0
        if not subs:
            return len(expr)
        length = len(expr)
        # from is a reserved keyword in python
        for from_, to in iteritems(subs):
            if len(from_) == 0:
                # this should never happen as 'from_' always has prefix '#'.
                # checking the condition to make the code agnostic to other validations.
                return
            reduced = re.sub(from_, '', expr)
            times = (len(expr) - len(reduced)) / len(from_)
            length -= times * len(from_)
            length += times * (len(to) if to else 0)
            expr = reduced
        return length

    @staticmethod
    def append_parameter_name(params, name):
        return params + ', ' + name if params else name

    @staticmethod
    def new_validation_exception(message):  # FIXME match DDB exception.
        return DaxValidationError(message)

    @staticmethod
    def validate_api_using_operation_model(operation_model, request):
        """
        validate using the dynamodb api operation model. This validate method adds some overhead (~100 microsec). The speed drops with the size of
        request to validate and also if the request is of write nature.

        This validation doesn't validate the correct use of expressions.
        for e.g. 1. The length of expression name has to be less then 65535 which is not validated by DDB service definition.
                 2. TransactItems should have upto 25 items(it validates that there is at least 1 item though)
                 3. In 1 TransactItem, there should only be 1 operation etc.
        DDB service definition validates that the shape of request is correct and required params are met.
        :param operation_model: operation model of the API function to be validated
        :param request: request data from user
        :return: None if it is valid else raise exception
        """
        input_shape = operation_model.input_shape
        if input_shape is not None:
            report = RequestValidator._PARAM_VALIDATOR.validate(request, operation_model.input_shape)
            if report.has_errors():
                raise RequestValidator.new_validation_exception(report.generate_report())
