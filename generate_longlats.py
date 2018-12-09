

def gen(cityid, regionid, upper_x, upper_y, lower_x, lower_y, radio_km):

    direction_x = -1 if (upper_x - lower_x > 0) else 1
    direction_y = -1 if (upper_y - lower_y > 0) else 1

    step_x = direction_x * 0.0180 * radio_km
    step_y = direction_y * 0.0180 * radio_km
    x = upper_x
    y = upper_y

    print ("Using: direction_x: %d, direction_y: %d" % (direction_x, direction_y))
    print ("Using: step_x: %.4f, step_y: %.4f" % (step_x, step_y))
    while y < direction_y * lower_y:

        x = upper_x
        while x > lower_x:

            #print("Pair: %.4f, %.4f" % (x, y))

            s = '{"name": "custom_locations", "values": [{"name":"(%.4f, .%.4f)","distance_unit":"kilometer","latitude":%.4f,"longitude":%.4f,"primary_city_id":%d,"radius":1,"region_id":%d,"country":"BR"}], "location_types":["home","recent"]},' % (x,y,cityid, regionid, x,y)
            print(s)
            x += step_x
        y += step_y




#gen(2.8889, -60.7762, 2.753, -60.6238, 1) # boavista
gen(269969, 460, -2.959286, -60.069741, -3.160124, -59.91250, 1)


