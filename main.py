from elasticsearch import Elasticsearch

import os

# OBLIGATOIRE
S_IMAGES_PATH = '/Users/sully/elastic/Twitter_2015_Imgs/.'
S_INDEX = 'twitter2015'
S_DOCTYPE = 'tweet'

# OPTIONS
# TIMESTAMP EN MS !
S_TIMESTAMP_BEGIN = ''
S_TIMESTAMP_END = ''

# 1 = ES ||| 2 = depuis fichier ||| 3 = verification inversee
N_OPTION = 3
S_PATH_FILE_ES = '/Users/sully/elastic/image_validation/data.txt'


class cHandleEs:
    def __init__(self):
        self.sCluster = ''

    def __init__(self, sName):
        self.sCluster = sName

    def connectionToEs(self):
        if len(self.sCluster):
            es = Elasticsearch(cluster=self.sCluster)
        else:
            es = Elasticsearch()
        return es

class cBatchId:
    def __init__(self, xEs, sIndexName, sDocTypeName):
        self.xEs = xEs
        self.sIndexName = sIndexName
        self.sDocTypeName = sDocTypeName
        self.nCurrentSize = 1
        self.nIndexSize = int(self.xEs.count(index = self.sIndexName)['count'])
        self.xIdPack = {}

        xFile = open("data.txt", "a")

        lFields = ['id_str']

        if S_TIMESTAMP_BEGIN and S_TIMESTAMP_END:
            xResponse = self.xEs.search(index=self.sIndexName, doc_type=self.sDocTypeName, scroll='10m',
                                        sort=['timestamp_ms:asc'], _source=lFields, stored_fields=lFields,
                                        body={'query': {'range': {'timestamp_ms': {'gte': S_TIMESTAMP_BEGIN,
                                                                                   'lte': S_TIMESTAMP_END}}}})
        elif S_TIMESTAMP_BEGIN:
            xResponse = self.xEs.search(index=self.sIndexName, doc_type=self.sDocTypeName, scroll='10m',
                                        sort=['timestamp_ms:asc'], _source=lFields, stored_fields=lFields,
                                        body={'query': {'range': {'timestamp_ms': {'gte': S_TIMESTAMP_BEGIN}}}})
        elif S_TIMESTAMP_END:
            xResponse = self.xEs.search(index=self.sIndexName, doc_type=self.sDocTypeName, scroll='10m',
                                        sort=['timestamp_ms:asc'], _source=lFields, stored_fields=lFields,
                                        body={'query': {'range': {'timestamp_ms': {'lte': S_TIMESTAMP_END}}}})
        else:
            xResponse = self.xEs.search(index=self.sIndexName, doc_type=self.sDocTypeName, scroll='10m',
                                        sort=['timestamp_ms:asc'], _source=lFields, stored_fields=lFields,
                                        body={'query': {'match_all': {}}})
        self.sScroll = xResponse['_scroll_id']
        nCmpt = 0
        for hit in xResponse['hits']['hits']:
            self.xIdPack.update({hit['_source']['id_str']:1})
            xFile.write(hit['_source']['id_str'] + '\n')
            self.nCurrentSize += 1
            nCmpt += 1

        self.nIndexSize = int(self.xEs.count(index=self.sIndexName)['count'])
        print('Taille index : ' + str(self.nIndexSize))

        while (nCmpt < self.nIndexSize):
            try:
                xResponse = self.xEs.scroll(scroll_id = self.sScroll, scroll ='10s')
                self.sScroll = xResponse['_scroll_id']
                for hit in xResponse['hits']['hits']:
                    self.xIdPack.update({hit['_source']['id_str']:1})
                    xFile.write(hit['_source']['id_str'] + '\n')
                    self.nCurrentSize += 1
                    if not (nCmpt % 1000):
                        print (nCmpt)
                    nCmpt += 1
                    if nCmpt > 506000:
                        print (nCmpt)
            except:
                break

        xFile.close()
        print ('FIN DE LA REQUETE ES')

    def getPack(self):
        return self.xIdPack


def validation():
    xFalse = []
    xTrue = []

    if N_OPTION == 1 :
        print('ID_STR DEPUIS INDEX_ES : ' + S_INDEX + '/' + S_DOCTYPE)
        xhandleES = cHandleEs('test_app')
        xEs = xhandleES.connectionToEs()
        xBatchId = cBatchId(xEs, S_INDEX, S_DOCTYPE)
        aIdEs = xBatchId.getPack()
    elif N_OPTION == 2 :
        print('ID_STR DEPUIS FICHIER : ' + S_PATH_FILE_ES)
        aIdEs = {}
        with open(S_PATH_FILE_ES) as xFileEs:
            for line in xFileEs:
                aIdEs.update({line[:-1]:1})



    aIdImage = [f[:-6] for f in os.listdir(S_IMAGES_PATH) if os.path.isfile(os.path.join(S_IMAGES_PATH, f))]

    """for t in aIdImage:
        print('local')
        print(t)
"""


#    it = aIdStr.iterator()
    """
    for t in aIdStr.getArray():
        print('ES')
        print(t)
    """
    nCmpt = 0
    print("Nombre d'images dans le dossier : " + str(len(aIdImage)))

    print('DEBUT DE LA COMPARAISON')
    for sCurrent in aIdImage:
        nCmpt += 1

        if not(sCurrent in aIdEs):
            xFalse.append(sCurrent)
        else:
            xTrue.append(sCurrent)
        if not (nCmpt % 1000):
            print(nCmpt)


    print("Nombre d'images dans le dossier : " + str(len(aIdImage)))
    print("Nombre d'images qui viennent de la base :" + str(len(xTrue)))
    print("Nombre d'images qui ne viennent pas de la base :" + str(len(xFalse)))

    return xFalse

def main():
    xFalse = validation()



if __name__ == '__main__':
    main()