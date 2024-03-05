import skg_main.skg_mgrs.connector_mgr as conn
from skg_main.skg_mgrs.skg_writer import Skg_Writer


def store_automaton(name: str, pov: str = None, start=None, end=None):
    driver = conn.get_driver()

    writer = Skg_Writer(driver)
    writer.write_automaton(name, pov, start, end)

    driver.close()


def delete_automaton(name: str = None, pov: str = None, start=None, end=None):
    driver = conn.get_driver()

    writer = Skg_Writer(driver)
    writer.cleanup(name, pov, start, end)

    driver.close()
