import re
class Metadata():
    def __init__(self, name, signature = dict(), distribute_on = ''):
        if self.check_output_name(name):
            self.name = name
        if self.check_signature(signature):
            self.signature = signature
        self.distribute_on_str = self.get_distribute_str(distribute_on)

    def check_output_name(self, name):
        if name is None:
            return True
        if not isinstance(name, str):
            return False
        regex = r"[a-zA-Z0-9_]+(\\.[a-zA-Z0-9_]+)?"
        matches = re.finditer(regex, name, re.S)
        for matchNum, match in enumerate(matches, start=1):
            if len(match.group()) != len(name):
                raise ValueError('Invalid output name')
            else:
                return True

    def check_signature(self, signature):
        if signature is None:
            raise ValueError('Null signature is not supported')
        elif isinstance(signature, dict):
            return True
        else:
            raise ValueError('Invalid signature type')

    def get_distribute_str(self, distribute_on):
        if distribute_on is None:
            return ''

        if isinstance(distribute_on, list):
            fields = ', '.join(distribute_on)
            #TODO case sensitive
            return 'DISTRIBUTED BY ('+fields+')'

        if isinstance(distribute_on, str):
            dist = distribute_on.upper()
            if dist == 'RANDOMLY':
                return 'DISTRIBUTED RANDOMLY'
            if dist == 'REPLICATED':
                return 'DISTRIBUTED REPLICATED'
            raise ValueError('Invalid distribute value')

        raise ValueError('invalid distributed type')