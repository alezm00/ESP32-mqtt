import _setup  # noqa: F401
import unittest

from mqttmanager import topic_matches


class TopicTests(unittest.TestCase):
    def test_matches(self):
        yes = [("a/b", "a/b"), ("a/+", "a/b"), ("+/b", "a/b"), ("a/#", "a/b/c"), ("a/#", "a"), ("#", "a/b"),
               ("+/+", "a/b"), ("a/+/c", "a/x/c"), ("+", "a"), ("a//c", "a//c"), ("a/+/c", "a//c")]
        no = [("a/b", "a/c"), ("a/+", "a/b/c"), ("a/+", "a"), ("+/b", "a/c"), ("a/b", "a"), ("a", "a/b"),
              ("#", "$SYS/x"), ("+/x", "$SYS/x"), ("a/+/c", "a/x/d")]
        for flt, topic in yes:
            self.assertTrue(topic_matches(flt, topic), (flt, topic))
        for flt, topic in no:
            self.assertFalse(topic_matches(flt, topic), (flt, topic))
        self.assertTrue(topic_matches("$SYS/#", "$SYS/broker/uptime"))


if __name__ == "__main__":
    unittest.main()
