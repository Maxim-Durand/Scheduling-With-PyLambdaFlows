"""
Disclaimer, this file isn't used, maybe soon =).
"""

class Processor():
    """
        This class will iterate through all tree dependances and create all lambda request for computing
        It also has to create associeted bucket and so on. 
    """
    def __init__(self, session):
        self.session = session

    def eval(self, target_op, input_dict):
        pass