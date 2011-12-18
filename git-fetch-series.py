#!/usr/bin/env python
from argparse import ArgumentParser
from nntplib import NNTP
from collections import namedtuple
from email.utils import parseaddr, parsedate
from email.header import make_header
from time import mktime, ctime

import re

Message = namedtuple('Message', ['number', 'subject', 'poster', 'date', 'msg_id', 'references'])

def limit(enum, limit=100):
    for c, x in enumerate(enum):
        if c > limit: raise StopIteration
        yield x


class Thread(object):

    @staticmethod
    def subject_identifier(msg):
        """
            Returns a prefix that we expect all patches to start with,
            having numbers replaced by X

            >>> subject_identifier("[PATCH 00/20] do something cool")
            [PATCH X/X]

            >>> subject_identifier("Re: [PATCH v2 1/2] do that better")
            Re: [PATCH v2 X/X]

            Messages are only considered part of the patch-set if they have
            the same subject_identifier
        """
        return re.sub(r"[0-9]+", "X", msg.subject.split("]")[0] + "]")

    @staticmethod
    def sortkey(msg):
        """
            Sort patches lexicographically, with the expedient change that
            numbers are sorted numerically.

            >>> sortkey("[PATCH 9/10]") < sortkey("[PATCH 10/10]")
            True

            >>> sortkey("[PATCH 9/10]")
            ["[PATCH ", 9, "/", 10, "]"]
        """
        splits = re.split(r"([0-9]+)", msg.subject.split("]")[0] + "]")

        for x in range(1, len(splits), 2):
            splits[x] = int(splits[x])

        return splits

    def __init__(self, start_msg):
        self.first = start_msg
        self.ignored_references = set(start_msg.references)
        self.ids = set([start_msg.msg_id])
        self.thread = [start_msg]
        self.thread_identifier = self.subject_identifier(start_msg)

    def should_include(self, msg):
        """
            Should this message be appended to this thread?
        """
        return (msg.poster == self.first.poster and
               self.subject_identifier(msg) == self.thread_identifier and
            (set(msg.references) - self.ignored_references) <= self.ids)


    def append(self, msg):
        self.thread.append(msg)
        self.ids.add(msg.msg_id)


    def in_order(self):
        return sorted(self.thread, key=self.sortkey)

class Archive(object):

    @staticmethod
    def get_email(header):
        return parseaddr(header)[1]

    @staticmethod
    def get_ctime(header):
        return 

    def __init__(self, group, server):
        self.conn = NNTP(server)
        resp, count, first, last, name = self.conn.group(group)

        self.first = int(first)
        self.last = int(last)

    def get_patch_series(self, start_id):
        start_id = int(start_id)

        messages = limit(self.messages_starting_from(start_id), 100)
        thread = Thread(messages.next())

        n_since_last = 0
        for message in messages:
            if thread.should_include(message):
                n_since_last = 0
                thread.append(message)

            elif n_since_last > 5:
                break

            else:
                n_since_last += 1

        else:
            raise RuntimeError('did not find end of thread in reasonable time')

        lines = []

        for message in thread.in_order():
            _, number, msg_id, body = self.conn.body(str(message.number))

            poster = parseaddr(message.poster)[0]
            date = ctime(mktime(parsedate(message.date)))
            lines.append("From %s %s" % (poster, date))

            lines.append("From: %s" % message.poster)
            lines.append("Subject: %s" % message.subject)
            lines.append("Date: %s" % message.date)
            lines.append("Message-Id: %s" % message.msg_id)
            lines.append("References: %s" % "\n\t".join(message.references))
            lines.append("")
            lines += body
            lines.append("")

        return "\n".join(lines)

    def messages_starting_from(self, start_id):
        """
            Generate all message headers starting from the given id and working upwards.
        """

        while start_id < self.last:
            next_id = min(start_id + 20, self.last)

            _, result = self.conn.xover(str(start_id), str(next_id))

            result.sort(key=lambda x: int(x[0]))

            for (number, subject, poster, date, msg_id, references, size, lines)  in result:
                yield Message(int(number), subject, poster, date, msg_id, references)

            start_id = next_id + 1

def main():

    parser = ArgumentParser(description="""
        git fetch-series downloads a patch series from usenet.

        It's only been tested with news.gmane.org's archive of gmane.comp.version-control.git,
        though in principal it could work with any archive that ensures all responses to a message
        appear after the message itself.
    """, epilog="""
    If your usenet server is hard to connect to, set up a netrc(5) file.
    """)

    parser.add_argument('-s', '--server', action='store', default='news.gmane.org',
            help="Which news server to connect to.")
    parser.add_argument('-n', '--newsgroup', action='store', default='gmane.comp.version-control.git',
            help="Which newsgroup to read out of")
    parser.add_argument('id',
            help="The message id or number of the first message in the patch series")

    opts = parser.parse_args()

    a = Archive(opts.newsgroup, opts.server)

    print a.get_patch_series(opts.id)


if __name__  == "__main__":
    main()
