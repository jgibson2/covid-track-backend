import s2sphere
from pynamodb.models import Model
from pynamodb.attributes import NumberAttribute, NumberSetAttribute
import itertools
from datetime import datetime
from flask import Flask
from flask_restful import Resource, Api

REGIONAL_LEVEL = 10
LOCAL_LEVEL = 16
DEFAULT_HOUR_LIMIT = 72

app = Flask(__name__)
api = Api(app)

class InfectionModel(Model):
    class Meta:
        table_name = 'infections'
    regional_cell = NumberAttribute(hash_key=True)
    local_cell = NumberAttribute(range_key=True)
    timestamps = NumberSetAttribute()

class InfectionTracker(Resource):
    def __init__(self):
        InfectionModel.create_table()


    def get(self):
        level = request.form.get('level', REGIONAL_LEVEL)
        hour_limit = request.form.get('hours', DEFAULT_HOUR_LIMIT)
        cell_id = request.form['id']
        data = []
        if level > REGIONAL_LEVEL:
            cells = [s2sphere.Cell(cell_id).parent(REGIONAL_LEVEL)]
        if level < REGIONAL_LEVEL:
            cells = s2sphere.Cell(cell_id).children(REGIONAL_LEVEL)
        else:
            cells = [s2sphere.Cell(cell_id)]
        for cell in cells:
            # get from database
            local_cells = [(i.local_cell,t) for t in i.timestamps for i in InfectionModel.query(cell.id())]
            # filter timestamps
            local_cells = [(c,ts) for c,ts in local_cells if datetime.now() - datetime.fromtimestamp(ts) < datetime.timedelta(hours=hour_limit)]
            # add to data
            data.extend(local_cells)
        return {'data': data}


    def put(self):
        level = request.form.get('level', LOCAL_LEVEL)
        data = request.form['data']
        timestamps = [ts for cell_id, ts in data]
        if level > LOCAL_LEVEL:
            cells = [s2sphere.Cell(cell_id).parent(LOCAL_LEVEL) for cell_id, timestamp in data]
        if level < LOCAL_LEVEL:
            cells = itertools.chain([s2sphere.Cell(cell_id).parent(LOCAL_LEVEL).id() for cell_id, timestamp in data])
        else:
            cells = [s2sphere.Cell(cell_id) for cell_id, timestamp in data]
        for cell, ts in zip(cells, timestamps):
            try:
                im = InfectionModel.get(cell.parent(REGIONAL_LEVEL).id(), cell.id())
                im.update(actions=[InfectionModel.timestamps.add(ts)])
            except InfectionModel.DoesNotExist:
                im = InfectionModel(regional_cell = cell.parent(REGIONAL_LEVEL).id(), local_cell = cell.id(), timestamps = {ts})
                im.save()
        return 'OK'


api.add_resource(InfectionTracker, '/', '/track')

if __name__ == '__main__':
    app.run(debug=True)
