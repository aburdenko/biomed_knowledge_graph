from google.cloud import _http
from google.cloud.client import ClientWithProject
from xml.etree import ElementTree as ET
from IPython.display import HTML
import pprint


class Connection(_http.JSONConnection):
  """Handles HTTP requests to GCP."""
  API_BASE_URL = 'https://healthcare.googleapis.com'
  API_VERSION = 'v1beta1'
  API_URL_TEMPLATE = '{api_base_url}/{api_version}/projects{path}'


class Client(ClientWithProject):
  """A client for accessing Cloud Healthcare NLP API.

  Args:
      project (Union[str, None]): The ID of the project
      region (str): The region the project resides in, e.g. us-central1,
  """

  def __init__(self,
               project=None,
               region='us-central1',
               credentials=None,
               http=None):
    self.region = region
    self.SCOPE = ('https://www.googleapis.com/auth/cloud-healthcare',)
    super(Client, self).__init__(project=project)
    self.path = '/{}/locations/{}/services/nlp'.format(self.project,
                                                       self.region)
    self._connection = Connection(self)

  def analyze_entities(self, document):
    """ Analyze the clinical entities a document with the Google Cloud

      Healthcare NLP API.

      Args:
        document (str): the medical document to analyze.

      Returns:
        Dict[str, Any]: the JSON response.
      """
    return self._connection.api_request(
        'POST',
        self.path + ':analyzeEntities',
        data={'document_content': document})



  def analyze_entities(self, document):
    """ Analyze the clinical entities a document with the Google Cloud

      Healthcare NLP API.

      Args:
        document (str): the medical document to analyze.

      Returns:
        Dict[str, Any]: the JSON response.
      """
    return self._connection.api_request(
        'POST',
        self.path + ':analyzeEntities',
        data={'document_content': document})        