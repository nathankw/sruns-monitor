import logging

from google.cloud import Firestore

import sruns_monitor as srm
from . import exceptions

logger = logging.getLogger(__name__)

class FirestoreCollection:

    def __init__(self, collname):
        """
        Args:
            collname: `str`. Name of the Firestore collection.
        """
        self.coll = firestore.Client().collection(name)

    def get(self, docid):
        """
        Retrieves a document in the Firestore collection that has the given entry name.

        Args:
            docid: `str`. The ID of a Firestore Document in the collection at hand.

        Returns: `dict`. 

        Raises:
            `sruns_monitor.exceptions.FirestoreDocumentMissing`: A corresponding Firestore document
                could not be found for the provided message.
        """

        logger.info(f"Querying Firestore for a document with ID '{docid}'")
        docref = self.firestore_coll.document(run_name # google.cloud.firestore_v1.document.DocumentReference
        doc = docref.get().to_dict() # dict
        if not doc:
            msg = f"No Firestore document exists with ID '{docid}'."
            raise exceptions.FirestoreDocumentMissing(msg)
        return doc

    def new(self, docid, payload):
        """
        Args:
            docid: `str`. The ID of a Firestore Document in the collection at hand.
            payload: `dict`. The properties to set in the Firestore Document.
        """
        self.coll.document(docid).set(payload)


    def update(self, docid, payload):
        """
        Args:
            docid: `str`. The ID of a Firestore Document in the collection at hand.
            payload: `dict`. The properties to set in the Firestore Document.
        """
        self.coll.document(docid).update(payload)


class SeqRunsFirestoreDoc:
    def __init__(self, data)
        """
        Args:
            data: `dict`. The key and values of a Firestore Document that abides by the sruns-monitor Firestore schema. 
        """
        self.data = data
        self.run_name = self.data.get(srm.FIRESTORE_ATTR_RUN_NAME)
       
    def get_storage_path(self):
        """
        Gets the path to the raw run directory in Google Storage. Does this by parsing the storage attribute of the Firestore document data. 
        """ 
        gs_rundir_path = self.data.get(srm.FIRESTORE_ATTR_STORAGE)
        if not gs_rundir_path:
            msg = f"Firestore document '{self.run_name}' doesn't have the storage path attribute '{srm.FIRESTORE_ATTR_STORAGE}' set!"
            msg += f" Did the sequencing run finish uploading to Google Storeage yet?"
            raise exceptions.FirestoreDocumentMissingStoragePath(msg)
        return gs_rundir_path
    
