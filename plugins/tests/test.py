class Test:
    def __init__(self, query, result, verification, parameters=None):
        self.query = query
        self.result = result
        self.verification = verification
        self.parameters = parameters or dict()
        
    def update_parameters(self, parameters):
        self.parameters = parameters
        
    def get_query(self):
        return self.query.format(**self.parameters)
    
    def check_against_output(self, output):
        return verification(self.result, output)
        