class Test:
    def __init__(self, query, result, verification, parameters=None):
        self.query = query
        self.verification = verification
        self.parameters = parameters or dict()
        
    def update_parameters(self, parameters):
        self.parameters = parameters
        
    def get_query(self):
        return self.query.format(**self.parameters)
    
    def check_against_records(self, records):
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError('Data quality check failed. Query returned no results')
            
        elif verification == 'has_rows':
            if records[0][0] == 0:
                raise ValueError(f'Data quality check failed:\n{self.get_query()}')
            else
                return
            
        elif verification == 'no_rows':
            if records[0][0] != 0:
                raise ValueError(f'Data quality check failed:\n{self.get_query()}')
            else
                return
            
        else:
            raise ValueError(f'Incorrect verification: {self.verification}')
            
        