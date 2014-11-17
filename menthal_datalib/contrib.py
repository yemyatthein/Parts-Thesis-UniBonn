import sys, math
from Queue import Queue
import pandas as pd
import numpy as np

def compute_correlation(times, current, target):
    df = pd.DataFrame(np.zeros([len(times), 2]), index=times, columns=['current', 'target'])
    df['current'] = current
    df['target'] = target
    return df['current'].corr(df['target'])

def compute_linear_regression(lsvalues):
    pass

def haversine(p1, p2):
    lat1, lon1, lat2, lon2 = p1[0], p1[1], p2[0], p2[1]
    R = 6372.8  # In Kilo-meter
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
 
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.sin(dLon / 2) * math.sin(dLon / 2) * math.cos(lat1) * math.cos(lat2)
    c = 2 * math.asin(math.sqrt(a))
    return R * c
    
class LocationDataAnalysis(object):
    GRID_COUNT = 10
    MAX_DEPTH = 5
    POINT_DIFFERENCE = 10
    POINT_DIFFERENCE_UP = 10
    IGNORE_DISTANCE = 1.0
    def __init__(self, config, coords, times, period_start, period_end, distance_func):
        self.config = config
        self.coords = coords
        self.times = times
        self.period_start = period_start
        self.period_end = period_end
        self.distance_func = distance_func
        if not distance_func:
            self.distance_func = haversine
        self.max_x = self.max_y = -(sys.maxint)
        self.min_x = self.min_y = sys.maxint
        for i, cd in enumerate(coords):
            self.max_x = max(self.max_x, cd[0])
            self.min_x = min(self.min_x, cd[0])
            self.max_y = max(self.max_y, cd[1])
            self.min_y = min(self.min_y, cd[1])
    def get_bounding_box(self):
        """ top-left, top-right, bottom-right, bottom-left """
        return [(self.min_x, self.max_y), (self.max_x, self.max_y), (self.max_x, self.min_y), (self.min_x, self.min_y)]
    def get_distance_travelled(self):
        total_distance = 0
        path = []
        path.append(self.coords[0])
        current = self.coords[0]
        for m, n in self.coords[1:]:
            d = self.distance_func(current, (m, n))
            if d > self.IGNORE_DISTANCE:
                path.append((m, n))
                current = (m, n)
                total_distance += d
        return path, total_distance
    def get_common_region(self):
        def test_values(nx, ny, gcells):
            if nx >= 0 and ny >= 0 and (nx, ny) in gcells:
                if abs(len(gcells[(cx, cy)]) - len(gcells[(nx, ny)])) < self.POINT_DIFFERENCE and \
                    abs(len(gcells[mx_key]) - len(gcells[(nx, ny)])) < self.POINT_DIFFERENCE_UP:
                    return (nx, ny)
            return None
        width = self.max_x - self.min_x
        height = self.max_y - self.min_y
        one_cell_width = width / float(self.GRID_COUNT)
        one_cell_height = height / float(self.GRID_COUNT)
        if width == 0. or height == 0.:
            if len(self.coords) > 0:
                return [(self.coords[0], self.coords[0])] * 4
            else:
                return []
        xintervals = {}
        for i in xrange(int(width / one_cell_width)):
            start = self.min_x + (i * one_cell_width)
            end = min(self.min_x + ((i + 1) * one_cell_width), self.max_x)
            xintervals[i] = [(start, end)]
        yintervals = {}
        for i in xrange(int(height / one_cell_height)):
            start = self.min_y + (i * one_cell_height)
            end = min(self.min_y + ((i + 1) * self.max_y), self.min_y)
            yintervals[i] = [(start, end)]
        grid_cells = {}
        for i in xrange(len(xintervals)):
            for j in xrange(len(yintervals)):
                grid_cells[(i, j)] = []
        for m, n in self.coords:
            diff_from_minx = m - self.min_x
            diff_from_miny = n - self.min_y
            gx = min(int(diff_from_minx / one_cell_width), len(xintervals) - 1)
            gy = min(int(diff_from_miny / one_cell_height), len(yintervals) -1)
            grid_cells[(gx, gy)].append((m, n))
        mx_key = 0
        for k, v in grid_cells.iteritems():
            if len(v) > len(grid_cells.get(mx_key, [])):
                mx_key = k
        all_cells = [mx_key]
        q = Queue()
        q.put((1, mx_key))
        seen = set()
        while not q.empty():
            depth, (cx, cy) = q.get()
            if depth > self.MAX_DEPTH:
                break
            # left
            nx = cx - 1
            ny = cy
            new = test_values(nx, ny, grid_cells)
            if new and new not in seen:
                seen.add(new)
                q.put((depth + 1, new))
                all_cells.append(new)
            # top
            nx = cx
            ny = cy + 1
            new = test_values(nx, ny, grid_cells)
            if new and new not in seen:
                seen.add(new)
                q.put((depth + 1, new))
                all_cells.append(new)
            # right
            nx = cx + 1
            ny = cy
            new = test_values(nx, ny, grid_cells)
            if new and new not in seen:
                seen.add(new)
                q.put((depth + 1, new))
                all_cells.append(new)
            # bottom
            nx = cx
            ny = cy - 1
            new = test_values(nx, ny, grid_cells)
            if new and new not in seen:
                seen.add(new)
                q.put((depth + 1, new))
                all_cells.append(new)
        xx = []
        yy = []
        for j, k in all_cells:
            for m, n in grid_cells[(j, k)]:
                xx.append(m)
                yy.append(n)
        minxx, minyy, maxxx, maxyy = min(xx), min(yy), max(xx), max(yy)
        """ top-left, top-right, bottom-right, bottom-left """
        return [(minxx, maxyy), (maxxx, maxyy), (maxxx, minyy), (minxx, minyy)]
