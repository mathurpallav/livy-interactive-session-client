import requests
import json
import time


class LivyInteractiveSessionClient:
    """
    LivyInteractiveSessionClient manages a Livy Interactive Session via Livy REST APIs.
    """

    def __init__(self, livy_url: str, config, jars):
        """
        Initializes a new LivyInteractiveSessionClient object.

        Args:
            livy_url (str): The URL of the Livy server (Like <cluster_ip:8998>)
            config (dict): Spark/Hadoop/Yarn properties you want to attach to the session.
            jars (list): List of JARs to be added to the Spark session.

        Raises:
            Exception: If the Livy session creation fails.
        """
        self.livy_url = livy_url
        self.jars = jars
        self.config = config
        self.session_id = self.create_session()
        self.headers = {'Content-Type': 'application/json'}

    def create_session(self):
        """
        Creates a new session using the Livy REST API.

        Returns:
            str: The ID of the created session.

        Raises:
            Exception: If the session creation fails.
        """
        data = {'kind': 'spark',
                'proxyUser': 'hadoop',
                'jars': self.jars,
                'conf': self.config
                }
        r = requests.post(f"{self.livy_url}/sessions", data=json.dumps(data), headers=self.headers)
        if r.status_code == requests.codes.created:
            session_id = r.json()['id']
            # Wait for session to be idle (Initialising)
            while True:
                req = requests.get(f"{self.livy_url}/sessions/{session_id}", headers=self.headers)
                session_state = req.json().get("state")
                if session_state == "idle":
                    return session_id
                elif session_state in ("starting", "not_started"):
                    time.sleep(15)
                    continue
                else:
                    raise Exception("Livy Error: Spark session initialization failed.")
        else:
            raise Exception(f"Livy Error: Session Post API failed with status code {r.status_code}.")

    def remove_session(self):
        """
        Kills/Removes the running interactive session using the Livy REST API.

        Raises:
            Exception: If the interactive session removal fails.
        """
        r = requests.delete(f"{self.livy_url}/sessions/{self.session_id}", headers=self.headers)
        if r.status_code != requests.codes.ok:
            raise requests.HTTPError(f"Livy Error: Session Delete API failed with status code {r.status_code}.")

    def submit_statement(self, statement):
        """
        Submits a Spark statement to the Livy Interactive Session for execution.

        Args:
            statement (str): The Spark statement to be executed (can contain multiple lines).

        Returns:
            str: The output of the executed statement.

        Raises:
            Exception: If the statement execution fails.
        """
        statement = {'code': statement}
        r = requests.post(f"{self.livy_url}/sessions/{self.session_id}/statements",
                          data=json.dumps(statement), headers=self.headers)
        if r.status_code == requests.codes.created:
            statement_id = r.json()['id']
            return self.get_output(statement_id)
        else:
            raise requests.HTTPError(f"Livy Error: Statement Post API failed with status code {r.status_code}.")

    def get_output(self, statement_id):
        """
        Retrieves the output of the completed spark statement

        Args:
            statement_id (str): The ID of the statement whose output is to be retrieved.

        Returns:
            str: The output of the executed statement.

        Raises:
            Exception: If the statement execution fails.
        """
        while True:
            r = requests.get(f"{self.livy_url}/sessions/{self.session_id}/statements/{statement_id}",
                             headers=self.headers)
            state = r.json().get("state")
            if state == "error":
                raise Exception("Spark Error: Statement execution failed")
            elif state in ("running", "waiting"):
                time.sleep(15)
                continue
            elif state == "available":
                output = r.json().get("output")
                status = output.get("status")
                if status == "ok":
                    text = output.get("data").get("text/plain")
                    return text
                else:
                    raise Exception("Livy/Spark Error: Statement execution failed.")
