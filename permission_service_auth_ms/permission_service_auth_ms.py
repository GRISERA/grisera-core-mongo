from typing import Union

import requests
from grisera import PermissionService


class PermissionServiceAuthMS(PermissionService):
    def get_permissions(self, user_id: Union[int, str]):
        """
        Send request to API to get permission_service_auth_ms for a user

        Args:
            user_id (int | str): Identifier of the resource

        Returns:
            Result of request as list of permission_service_auth_ms objects
        """
        headers = {
            "Authorization": f"Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaTEhXMXNkSmwzREU0UWFwVmZ5OFVYelgzRDh0czlQTlFSYk52YnI0Rnh3In0.eyJleHAiOjE3MzMzNjgwMTIsImlhdCI6MTczMzMzMjAxMiwianRpIjoiZGM4ZjQ4N2MtNWYzMi00OGRhLWEwZjgtMmIyZjNkYWM0YTI0IiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDkwL3JlYWxtcy9ncmlzZXJhIiwic3ViIjoiZWQ5YmEyZmEtMTYyZS00NjZiLTk1NjItNDI0MTkxYjY4YmQzIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiZ3Jpc2VyYS1hcGkiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbIi8qIl0sInNjb3BlIjoicHJvZmlsZSBlbWFpbCIsImNsaWVudEhvc3QiOiIxNzIuMjAuMC4xIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtZ3Jpc2VyYS1hcGkiLCJjbGllbnRBZGRyZXNzIjoiMTcyLjIwLjAuMSIsImNsaWVudF9pZCI6ImdyaXNlcmEtYXBpIn0.SkL_3fguaumU-E59hZIjO_rviS0EHte1LufKgqRcxaqPMhuchn1EjgI1gtIhlOa5e5UBeMynQm38tqHZVQW2uLuGZ1pPmLa-3EqJRx1-xzvslEbxIXfXwnEzs0m19kvksl1j0YUoaA1K1bL7a0ZQikOVXi3PRHRyhuyKVFGyMhI1Y_MF-Cty1K-RqRxZUTftUAMpzUL2L_rKJSWPcWHYW-CGlFvUwbq8f3BHfXuz20uh_brsbUmrhN_jEpJr6suuOUWxfFYDalgZEKdhpUq3JbiU_66hV1SGesHm6yIo1pf9RceK80BDEqsVGj0zFlRS3V411TzfGVNovA5-r68f_g"
        }
        # TODO: token from client credentials
        # TODO: url as env variable
        response = requests.get('http://localhost:8081/api/permissions/507f1f77bcf86cd799439011', headers=headers)
        return response.json()
