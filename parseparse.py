#! /usr/bin/python3

import csv
import json
import logging
import multiprocessing

logging.basicConfig(level=logging.DEBUG)

class DataFactory(object):
    def load_data(self, name, data_folder='export_Feb_19/'):
        return (item for item in json.load(open(data_folder + name + '.json'))['results'])

    def get_instance(self, data):
        if data == 'Gestures':
            return self.load_data('Gestures')
        if data == 'People':
            return self.load_data('People')
        if data == 'PersonGestures':
            return self.load_data('PersonGestures')
        if data == 'SensorValues':
            return self.load_data('SensorValues')
        if data == 'Person_PersonGestures':
            return self.load_data('_Join:person:PersonGestures')
        if data == 'Gesture_PersonGestures':
            return self.load_data('_Join:gesture:PersonGestures')
        if data == 'PersonGesture_SensorValues':
            return self.load_data('_Join:person_gesture:SensorValues')
        else:
            raise Exception('Invalid File')


def sensor_values(person_gesture):
    pgsvs = (
        pgsv['owningId'] for pgsv in DataFactory().get_instance('PersonGesture_SensorValues') 
        if pgsv['relatedId'] == person_gesture['objectId']
    )
    pgsvs = list(pgsvs)
    svs = (sv for sv in DataFactory().get_instance('SensorValues') if sv['objectId'] in pgsvs)

    '''
    sorting list in order of timestamps 
    -> http://stackoverflow.com/questions/72899/how-do-i-sort-a-list-of-dictionaries-by-values-of-the-dictionary-in-python
    '''
    return  [ 
        [sv['time'], sv['ax'], sv['ay'], sv['az']] 
        for sv in sorted(svs, key=lambda k: k['time'])
    ] 


def save_file(file_name, sensor_values, data_folder='parsed'):
    with open(data_folder+'/'+file_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(sensor_values)


def file_gen(person_gesture):
    object_id = person_gesture['objectId']
    logging.info('file_gen for PersonGesture id ' + object_id)
    gesture_name = [
        [
            g['tag'] for g in DataFactory().get_instance('Gestures') 
            if g['objectId'] == gpg['relatedId']
        ] for gpg in DataFactory().get_instance('Gesture_PersonGestures') 
        if gpg['owningId'] == object_id
    ][0][0]

    person_name = [
        [
            p['name'].lower().replace(' ', '_') for p in DataFactory().get_instance('People') 
            if p['objectId'] == gpg['relatedId']
        ] for gpg in DataFactory().get_instance('Person_PersonGestures') 
        if gpg['owningId'] == object_id
    ][0][0]

    file_name = gesture_name+'-'+person_name+'-'+str(person_gesture['sample_number'])
    logging.info('file_name ' + file_name)

    svs = sensor_values(person_gesture)
    logging.info('sensor_values ' + str(len(svs)) + ' samples')

    save_file(file_name, svs)


def main():
    logging.info("Main")

    pool = multiprocessing.Pool(processes=5)
    pool.map_async(file_gen, DataFactory().get_instance('PersonGestures'))
    pool.close()
    pool.join()
    # for pg in DataFactory().get_instance('PersonGestures'):
    #     # if pg['objectId'] == 'CKZkfmv9JS':
    #     file_gen(pg)


if __name__ == '__main__':
    main()
