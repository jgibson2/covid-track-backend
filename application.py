import s2sphere
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
        level = int(request.args.get('level', REGIONAL_LEVEL))
        print(f'LEVEL: {level}')
        hour_limit = float(request.args.get('hours', DEFAULT_HOUR_LIMIT))
        print(f'HOUR_LIMIT: {hour_limit}')
        cell_id = request.args['id']
        print(f'CELL_ID: {cell_id}')
        data = []
        if level > REGIONAL_LEVEL:
            cells = [s2sphere.CellId(int(cell_id)).parent(REGIONAL_LEVEL)]
        if level < REGIONAL_LEVEL:
            cells = s2sphere.CellId(int(cell_id)).children(REGIONAL_LEVEL)
        else:
            cells = [s2sphere.CellId(int(cell_id))]
        for cell in cells:
            # get from database
            local_cells = []
            results = InfectionModel.query(cell.id())
            for r in results:
                local_cells.extend([(r.local_cell, t) for t in r.timestamps])
            # filter timestamps
            local_cells = [(c,ts) for c,ts in local_cells if datetime.now() - datetime.fromtimestamp(int(ts) / 1000) < timedelta(hours=hour_limit)]
            # add to data
            data.extend(local_cells)
        return {'data': data}


    def post(self):
        level = int(request.get_json().get('level', LOCAL_LEVEL))
        print(f'LEVEL: {level}')
        data = request.get_json().get('data')
        print(f'DATA: {data}')
        print(f'DATA[0]: {data[0]}')
        timestamps = [ts for cell_id, ts in data]
        if level > LOCAL_LEVEL:
            cells = [s2sphere.CellId(int(cell_id)).parent(LOCAL_LEVEL) for cell_id, timestamp in data]
        if level < LOCAL_LEVEL:
            cells = itertools.chain([s2sphere.CellId(int(cell_id)).parent(LOCAL_LEVEL).id() for cell_id, timestamp in data])
        else:
            cells = [s2sphere.CellId(int(cell_id)) for cell_id, timestamp in data]
        for cell, ts in zip(cells, timestamps):
            try:
                im = InfectionModel.get(cell.parent(REGIONAL_LEVEL).id(), cell.id())
                im.update(actions=[InfectionModel.timestamps.add(ts)])
            except InfectionModel.DoesNotExist:
                im = InfectionModel(regional_cell = cell.parent(REGIONAL_LEVEL).id(), local_cell = cell.id(), timestamps = {ts})
                im.save()
        return make_response('OK', 200)


api.add_resource(InfectionTracker, '/', '/track')

if __name__ == '__main__':
    app.run(debug=True)
