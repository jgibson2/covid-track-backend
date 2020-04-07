import s2sphere
import numpy as np

from pynamodb.models import Model
from pynamodb.attributes import NumberAttribute, NumberSetAttribute
import itertools
from datetime import datetime, timedelta
from flask import Flask, request, make_response
from flask_restful import Resource, Api

REGIONAL_LEVEL = 10
LOCAL_LEVEL = 16
DEFAULT_HOUR_LIMIT = 72

app = Flask(__name__)
api = Api(app)


class InfectionModel(Model):
    class Meta:
        table_name = 'infections'
        host = "http://localhost:8000"
    regional_cell = NumberAttribute(hash_key=True)
    local_cell = NumberAttribute(range_key=True)
    timestamps = NumberSetAttribute()


class InfectionTracker(Resource):
    def __init__(self):
        InfectionModel.create_table(read_capacity_units=1, write_capacity_units=1)


    def get(self):
        hour_limit = float(request.args.get('hours', DEFAULT_HOUR_LIMIT))
        print(f'HOUR_LIMIT: {hour_limit}')
        cell_id = request.args['cell_token']
        print(f'CELL_ID: {cell_id}')
        level = s2sphere.CellId.from_token(cell_id).level()
        print(f'LEVEL: {level}')
        data = []
        cells = itertools.chain(map(lambda c: self.level_cell(c, REGIONAL_LEVEL), [cell_id]))
        for cell in cells:
            # get from database
            local_cells = []
            results = InfectionModel.query(cell.id())
            for r in results:
                local_cells.extend([(r.local_cell, t) for t in r.timestamps])
            # filter timestamps
            local_cells = [{'cell_token': s2sphere.CellId(c).to_token(), 'timestamp': ts} for c,ts in local_cells if datetime.now() - datetime.fromtimestamp(int(ts) / 1000) < timedelta(hours=hour_limit)]
            # add to data
            data.extend(local_cells)
        return {'data': data}


    def post(self):
        level = int(request.get_json().get('level', LOCAL_LEVEL))
        print(f'LEVEL: {level}')
        data = request.get_json().get('data')
        print(f'DATA: {data}')
        timestamps = [d['timestamp'] for d in data]
        cells = itertools.chain(map(lambda c: self.level_cell(c, LOCAL_LEVEL), [d['cell_token'] for d in data]))
        for cell, ts in zip(cells, timestamps):
            time = int(np.random.normal(ts, 1E5))
            try:
                im = InfectionModel.get(cell.parent(REGIONAL_LEVEL).id(), cell.id())
                im.update(actions=[InfectionModel.timestamps.add(time)])
            except InfectionModel.DoesNotExist:
                im = InfectionModel(regional_cell = cell.parent(REGIONAL_LEVEL).id(), local_cell = cell.id(), timestamps = {time})
                im.save()
        return make_response('OK', 200)


    def level_cell(self, cell_id, desired_level):
        c = s2sphere.CellId.from_token(cell_id) 
        if c.level() > desired_level:
            c = s2sphere.CellId.from_token(cell_id).parent(desired_level)
        elif c.level() < desired_level:
            c = list(s2sphere.CellId.from_token(cell_id).children(desired_level))
        return c


api.add_resource(InfectionTracker, '/', '/track')

if __name__ == '__main__':
    app.run(debug=True)
