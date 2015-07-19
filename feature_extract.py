import csv
import os
import operator
import numpy
import scipy.signal as signal
import matplotlib.pyplot as pyplot


extract = [
    "left_wave", 
    "right_wave"
]

def save_file(features, file_name="features"):
    with open(file_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(features)


def maximas(np_arr):
    return signal.argrelextrema(np_arr, numpy.greater)


def top_amps(np_arr, minimum=5 ,reverse_order=True):
    # find max amp index n_max is the array of horizontal (X axis) values for which Y Axis has a local maxima 
    indices = maximas(np_arr)
    # find value at that maxima
    vals = np_arr[tuple(indices), ]
    # make a dict with key as freq. bin (or X value in last to last step) and value as Y Axis value and sort them in decreasing order of magnitude
    top_amps =  sorted(dict(zip(indices[0], vals[0])).items(), key=operator.itemgetter(1), reverse=reverse_order)

    if (len(top_amps) >= minimum):
        return top_amps[0:minimum]
    else:
        return None


def flattened_top_amps(np_arr, minimum=5, reverse_order=True):
    try:
        return [item for sublist in top_amps(np_arr, minimum, reverse_order) for item in sublist]
    except TypeError:
        return None


def compute_features(filename, gesture_name):
    data = numpy.loadtxt("parsed/"+filename, delimiter=",")
    if (data.any()):    
        motion = data[:, 1:4]
        motion_fft_mag = abs(numpy.fft.fft(motion))

        # seg. x, y, z fft
        x_mag = motion_fft_mag[:, 0]
        y_mag = motion_fft_mag[:, 1]
        z_mag = motion_fft_mag[:, 2]

        f_x = flattened_top_amps(x_mag)
        f_y = flattened_top_amps(y_mag)
        f_z = flattened_top_amps(z_mag)

        try:
            return f_x + f_y + f_z + [extract.index(gesture_name)]
        except TypeError:
            pass

        # print("x", len(x.keys()))
        # print("y", len(y.keys()))
        # print("z", len(z.keys()))




def main(): 
    a = [
        [
            compute_features(filename, e) for e in extract if e+'-' in filename
        ] 
        for filename in os.listdir(os.getcwd()+"/parsed")
    ]

    a = [x[0] for x in a if x and x[0] != None]

    save_file(a)

            

if __name__ == '__main__':
    main()