__author__ = "Jeremy Nelson"
import falcon

rest = falcon.API()

class Triple:

    def on_get(self, req, resp, subj, pred, obj):
        resp.body = None
        

rest.add_route('/<subj>/<pred>/<obj>',
    Triple())
