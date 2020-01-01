class ConfigException(Exception):                                                                      
    pass                                                                                               
                                                                                                       
                                                                                                       
class FirestoreDocumentMissing(Exception):                                                             
    """                                                                                                
    Raised when we are looking for a particular Firestore document but it doesn't yet exist.           
    """                                                                                                
    pass                                                                                               

class FirestoreDocumentMissingStoragePath(Exception):                                                             
    """                                                                                                
    Raised when a Firestore document is missing the storage location while it is expected. 
    """                                                                                                
    pass                                                                                               
                                                                                                       
class MissingTarfile(Exception):                                                                       
    pass   
