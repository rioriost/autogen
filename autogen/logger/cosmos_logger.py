from __future__ import annotations

import json
import logging
import os
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import sys
import uuid

from autogen.logger.base_logger import BaseLogger
from autogen.logger.logger_utils import get_current_ts, to_dict

from openai import OpenAI, AzureOpenAI
from openai.types.chat import ChatCompletion
from typing import Dict, TYPE_CHECKING, Union


if TYPE_CHECKING:
    from autogen import ConversableAgent, OpenAIWrapper


# this is a pointer to the module object instance itself
this = sys.modules[__name__]
this._session_id = None
logger = logging.getLogger(__name__)

__all__ = ("CosmosLogger",)


class CosmosLogger(BaseLogger):
    def __init__(self, config):
        self.chat_container = None
        self.agent_container = None
        self.wrapper_container = None
        self.client_container = None
        self.config = config

    def start(self) -> str:
        endpoint = self.config["cosmos_endpoint"] if "cosmos_endpoint" in self.config else "https://localhost:8081"
        key = self.config["cosmos_key"] if "cosmos_key" in self.config else "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbJyK8MkMqKwVb8ySxwqL6g=="
        dbname = self.config["cosmos_dbname"] if "cosmos_dbname" in self.config else "logs"
        chat_container_name = self.config["cosmos_chat_container"] if "cosmos_chat_container" in self.config else "chat_completions"
        agent_container_name = self.config["cosmos_agent_container"] if "cosmos_agent_container" in self.config else "agents"
        wrapper_container_name = self.config["cosmos_wrapper_container"] if "cosmos_wrapper_container" in self.config else "oai_wrappers"
        client_container_name = self.config["cosmos_client_container"] if "cosmos_client_container" in self.config else "oai_clients"
        this._session_id = str(uuid.uuid4())

        # Get a client
        try:
            client = CosmosClient(url=endpoint, credential=key)
        except:
            logger.error('Could not connect to Cosmos DB')
            raise ConnectionError
        # Get a database and containers
        try:
            database = client.create_database_if_not_exists(id=dbname)
            # Need to check if the partition keys are correct
            self.chat_container = database.create_container_if_not_exists(id=chat_container_name, partition_key=PartitionKey(path='/invocation_id'))
            self.agent_container = database.create_container_if_not_exists(id=agent_container_name, partition_key=PartitionKey(path='/agent_id'))
            self.wrapper_container = database.create_container_if_not_exists(id=wrapper_container_name, partition_key=PartitionKey(path='/wrapper_id'))
            self.client_container = database.create_container_if_not_exists(id=client_container_name, partition_key=PartitionKey(path='/client_id'))
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f'Could not find database {dbname} or containers. {0}'.format(e.message))
        finally:
            return this._session_id

    def log_chat_completion(
        self,
        invocation_id: uuid.UUID,
        client_id: int,
        wrapper_id: int,
        request: Dict,
        response: Union[str, ChatCompletion],
        is_cached: int,
        cost: float,
        start_time: str,
    ) -> None:
        if self.chat_container is None:
            return

        end_time = get_current_ts()

        if response is None or isinstance(response, str):
            response_messages = json.dumps({"response": response})
        else:
            response_messages = json.dumps(to_dict(response), indent=4)

        item = {
            "invocation_id": invocation_id,
            "client_id": client_id,
            "wrapper_id": wrapper_id,
            "session_id": this._session_id,
            "request": json.dumps(request),
            "response": response_messages,
            "is_cached": is_cached,
            "cost": cost,
            "start_time": start_time,
            "end_time": end_time,
        }
        try:
            self.chat_container.create_item(body=item)
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"[CosmosLogger] log_chat_completion error: {e.message}")

    def log_new_agent(self, agent: ConversableAgent, init_args: Dict) -> None:
        from autogen import Agent

        if self.agent_container is None:
            return

        args = to_dict(
            init_args,
            exclude=("self", "__class__", "api_key", "organization", "base_url", "azure_endpoint"),
            no_recursive=(Agent),
        )

        # We do an upsert since both the superclass and subclass may call this method (in that order)
        item = {
            "agent_id": id(agent),
            "wrapper_id": agent.client.wrapper_id if hasattr(agent, "client") and agent.client is not None else "",
            "session_id": this._session_id,
            "name": agent.name if hasattr(agent, "name") and agent.name is not None else "",
            "class": type(agent).__name__,
            "is_cached": is_cached,
            "init_args": json.dumps(args),
            "timestamp": get_current_ts(),
        }
        try:
            self.agent_container.upsert_item(body=item)
        except exceptions.CosmosResourceExistsError:
            logger.error(f"[CosmosLogger] log_new_agent error: {e}")

    def log_new_wrapper(self, wrapper: OpenAIWrapper, init_args: Dict) -> None:
        if self.wrapper_container is None:
            return

        args = to_dict(
            init_args, exclude=("self", "__class__", "api_key", "organization", "base_url", "azure_endpoint")
        )

        item = {
            "wrapper_id": id(wrapper),
            "session_id": this._session_id,
            "init_args": json.dumps(args),
            "timestamp": get_current_ts(),
        }
        try:
            self.wrapper_container.create_item(body=item)
        except exceptions.CosmosHttpResponseError:
            pass

    def log_new_client(self, client: Union[AzureOpenAI, OpenAI], wrapper: OpenAIWrapper, init_args: Dict) -> None:
        if self.client_container is None:
            return

        args = to_dict(
            init_args, exclude=("self", "__class__", "api_key", "organization", "base_url", "azure_endpoint")
        )

        item = {
            "client_id": id(client),
            "wrapper_id": id(wrapper),
            "session_id": this._session_id,
            "class": type(client).__name__,
            "init_args": json.dumps(args),
            "timestamp": get_current_ts(),
        }
        try:
            self.client_container.create_item(body=item)
        except exceptions.CosmosHttpResponseError:
            pass

    def stop(self) -> None:
        if self.chat_container:
            self.chat_container = None
        if self.agent_container:
            self.agent_container = None
        if self.wrapper_container:
            self.wrapper_container = None
        if self.client_container:
            self.client_container = None
