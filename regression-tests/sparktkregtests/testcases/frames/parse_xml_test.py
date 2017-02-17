""" Test XML multi-line parsing.  """

import unittest

from sparktkregtests.lib import sparktk_test


class XMLReadTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(XMLReadTest, self).setUp()

        self.dangle_tag_xml = self.get_file("xml_dangle_tag.xml")
        self.overlap_2_xml = self.get_file("xml_overlap_2level.xml")
        self.overlap_inner_xml = self.get_file("xml_overlap_inner.xml")
        self.overlap_xml = self.get_file("xml_overlap.xml")
        self.comment_xml = self.get_file("xml_comment.xml")
        self.doc_xml = self.get_file("xml_doc.xml")
        self.smoke_xml = self.get_file("xml_smoke.xml")

    @unittest.skip("sparktk: nodes are dropped from import_xml when invalid values found")
    def test_xml_good_001(self):
        """ Check basic happy-path XML input """
        frame = self.context.frame.import_xml(self.smoke_xml, "node")

        take = frame.take(20)
        self.assertEqual(take[2], 300, "Node3 value incorrect")

    @unittest.skip("sparktk: nodes are dropped from import_xml when invalid values found")
    def test_xml_comment(self):
        """ Check basic happy-path XML input """
        frame = self.context.frame.import_xml(self.comment_xml, "node")

        take = frame.take(20)
        self.assertEqual(take[2][1], 300, "Node3 value incorrect")

    def test_xml_square(self):
        """ Validate the example given in the user documentation.  """
        frame = self.context.frame.import_xml(self.doc_xml, "square")

        # Now we will want to parse our values out of the xml file.
        # To do this we will use the add_columns method::

        def parse_square_xml(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row)
            return (ele.get("color"),
                    ele.find("name").text,
                    ele.find("size").text)

        take = frame.take(20)
        self.assertEqual(parse_square_xml(take[0][0])[2], "3", "Square size incorrect")
    
    def test_xml_add_columns(self):
        """validate adding cols to xml frame"""
        frame = self.context.frame.import_xml(self.doc_xml, "square")
        
        def parse_square_xml(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return [ele.get("color"),
                    ele.find("name").text,
                    ele.find("size").text]

        frame.add_columns(parse_square_xml, [("elements", str)])
        frame.count()


    @unittest.skip("sparktk: import_xml does not error when overlapped blocks at top level")
    def test_xml_overlap_outer(self):
        """ Reject overlapped blocks at top level """
        frame = self.context.frame.import_xml(self.overlap_xml, "block1")

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaisesRegexp(Exception, "foo"):
            frame.add_columns(parse_xml_1col, [("name", str)])
    
    @unittest.skip("sparktk: import_xml does not error when overlapped blocks at top level")
    def test_xml_overlap_inner(self):
        """Reject overlapped blocks nested within blocks, otherwise legal"""
        frame = self.context.frame.import_xml(self.overlap_inner_xml, "node1")

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaisesRegexp(Exception, "foo"):
            frame.add_columns(parse_xml_1col, [("name", str)])
    
    @unittest.skip("sparktk: import_xml does not error when overlapped blocks at top level")
    def test_xml_overlap_2level(self):
        """ Reject overlapped blocks through 2 levels"""
        frame = self.context.frame.import_xml(self.overlap_2_xml, "block2")

        def parse_xml_1col(row):
            import xml.etree.ElementTree as eTree
            ele = eTree.fromstring(row[0])
            return ele.find("name").text

        with self.assertRaisesRegexp(Exception, "foo"):
            frame.add_columns(parse_xml_1col, [("name", str)])

    def test_xml_dangle(self):
        """ Accept a partial block.  """
        frame = self.context.frame.import_xml(self.dangle_tag_xml, "node")

        self.assertEqual(frame.count(), 1)


if __name__ == "__main__":
    unittest.main()
