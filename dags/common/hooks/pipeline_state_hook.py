"""Pipeline state hook module."""

from typing import Union

from airflow.contrib.hooks.aws_dynamodb_hook import AwsDynamoDBHook


class PipelineStateHook(AwsDynamoDBHook):
    """Pipeline stats storage hook.

    Parameters
    ----------
    state_table_name
        Pipeline state DynamoDB table name.
    args, kwargs
        Standard AWS hook paramters.

    """
    # pylint: disable=abstract-method

    def __init__(self, state_table_name: str, *args, **kwargs):
        super().__init__(
            table_name=state_table_name,
            table_keys=['stateKey'],
            *args, **kwargs
        )

        self._state_table = self.get_conn().Table(self.table_name)


    def get_state(self, key: str) -> Union[str, None]:
        """Get currently saved state for the specified key, if any.

        Parameters
        ----------
        key
            State record key.

        Returns
        -------
        str or None
            The state value, or `None` if was never saved.

        """

        self.log.info(
            "getting pipeline state from %s, key=%s",
            self.table_name, key
        )
        res = self._state_table.get_item(
            Key={'stateKey': key},
            ConsistentRead=True
        )
        self.log.info("got response: %s", res)

        return res['Item']['value'] if 'Item' in res else None


    def save_state(self, key: str, value: str) -> None:
        """Save state associated with the specified key.

        Parameters
        ----------
        key
            State record key.
        value
            The state value.

        """

        self.log.info(
            "saving pipeline state in %s, key=%s, value=%s",
            self.table_name, key, value
        )
        res = self._state_table.put_item(
            Item={'stateKey': key, 'value': value}
        )
        self.log.info("got response: %s", res)
