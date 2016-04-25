

class DatacubeConstants(object):
    """Constants and utility methods related to the Datacube
    """

    CUBE_PROJECTION = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,' \
                      'AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],' \
                      'UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'

    PIXEL_SIZE_DEGREES = 0.00025

    @staticmethod
    def getGeoTransform(lat, lon):
        """Return the GeoTransform applicable to the supplied datacube cell origin ( lat, lon)
        """
        return (lon, DatacubeConstants.PIXEL_SIZE_DEGREES, 0.0, \
                lat+1, 0.0, -DatacubeConstants.PIXEL_SIZE_DEGREES)
