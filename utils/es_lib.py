import requests

class ESLib:
    def __init__(self, index, host="http://localhost:9200"):
        self.host = host
        self.index = index
        self.session = requests.Session()
    
    def requestES(self, methode, path, data):
        url = f"{self.host}{path}"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        request = requests.Request(methode, url, json=data, headers=headers)
        prep_request = self.session.prepare_request(request)
        response = self.session.send(prep_request)
        return (response.status_code, response.json())
        
    def getES(self, path, data):
        return self.requestES('GET', path, data)
    
    def postES(self, path, data):
        return self.requestES('POST', path, data)

    def deleteES(self, path, data):
        return self.requestES('DELETE', path, data)

    def deleteScroll(self, scroll_id):
        return self.deleteES("/_search/scroll", {'scroll_id': [scroll_id]})
    
    def search(self, query, size=10):
        data = {'size': size, 'query': query, 'sort': ["_doc"]}
        return self._search(data)

    def _search(self, data):
        return self.postES(f"/{self.index}/_search?scroll=1m", data)
    
    def scroll(self, scroll_id, keep_alive='1m'):
        data = {'scroll_id': scroll_id, 'scroll': keep_alive}
        return self.postES(f"/_search/scroll", data)

    def aggregate(self, name, field, min_doc_count=2, size=10000):
        data = {'query': {'match_all': {}}, 'aggs': {}, 'size': size}
        data['aggs'][name] = {"terms": {"field": field, "min_doc_count": min_doc_count}}
        return self._search(data)
    