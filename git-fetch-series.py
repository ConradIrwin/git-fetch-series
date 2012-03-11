#!/usr/bin/env python
from argparse import ArgumentParser
from nntplib import NNTP, NNTPError
from collections import namedtuple
from email.utils import parseaddr, parsedate
from email.header import make_header
from time import mktime, ctime
from sys import stdout, stderr

import re

Message = namedtuple('Message', ['number', 'subject', 'poster', 'date', 'msg_id', 'references'])

class FatalError(RuntimeError):
    pass

def limit(generator, limit):
    """
        return only the first limit items from the given generator
    """
    for c, x in enumerate(generator):
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

            Importantly, adding a Re: should break the sequence.
        """
        return re.sub(r"[0-9]+/", "X/", msg.subject.split("]")[0] + "]")

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
        splits = re.split(r"([0-9]+)", msg.subject)

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

            TODO: This logic is a bit "meh", it would be nice to support
            numbered sequences of patches with the threading a bit squiffy,
            as well as well-threaded sets of patches with no numbers.
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
    def is_diff(body):
        return bool([line for line in body if line.startswith("diff ")])

    def __init__(self, group, server):
        self.conn = NNTP(server)
        resp, count, first, last, name = self.conn.group(group)

        self.group = group
        self.server = server
        self.first = int(first)
        self.last = int(last)

    def get_number_from_user(self, msg_id):
        """
            Convert something the user might input into a message id.

            These are:
            # An NNTP message number
            # A gmane link that includes the NNTP message number
            # The original Message-Id header of the message.

            NOTE: gmane's doesn't include the message number in STAT requests
            that involve only the Message-Id (hence the convolution of getting
            all the headers).
        """
        msg_id = re.sub(r".*gmane.org/gmane.comp.version-control.git/([0-9]+).*", r"\1", str(msg_id))
        _, n, id, result = self.conn.head(msg_id)

        for header in result:
            m = re.match(r"Xref: .*:([0-9]+)\s*$", header, re.I)
            if m:
                return int(m.group(1))
        else:
            raise FatalError("No (or bad) Xref header for message '%s'" % msg_id)

    def get_patch_series(self, user_input, search_limit=100):
        """
            Given an NNTP message number or a Message-Id header return
            an mbox containing the patches introduced by the author of that message.

            This handles the case where the threading is right *and* the patches
            are numbered in a simple scheme:

            [PATCH] this patch has no replies and stands on its own

            [PATCH 0/2] this is an introduction to the series
              |- [PATCH 1/2] the first commit
              |- [PATCH 2/2] the second commit

            [PATCH 1/3] this is the first commit
              |- [PATCH 2/3] and this is the second
                   |- [PATCH 3/3] and this is the third

            TODO: it would be nice to make the search more efficient, we can
            use the numbers in [PATCH <foo>/<bar>] to stop early.
        """

        start_id = self.get_number_from_user(user_input)

        messages = limit(self.messages_starting_from(start_id), search_limit)
        try:
            thread = Thread(messages.next())
        except StopIteration:
            raise FatalError("No message at id '%s' using XOVER")

        n_since_last = 0
        for message in messages:
            if n_since_last > 5:
                break

            elif thread.should_include(message):
                n_since_last = 0
                thread.append(message)

            else:
                n_since_last += 1

        else:
            raise FatalError('did not find end of series within %s messages', search_limit)

        for message in self.xover(start_id - 5, start_id -1):
            if thread.should_include(message):
                thread.append(message)

        return self.mboxify(thread)

    def mboxify(self, thread):
        """
            Convert a thread into an mbox for application via git-am.
        """
        lines = []

        for message in thread.in_order():
            _, number, msg_id, body = self.conn.body(str(message.number))

            # git-am doesn't like empty patches very much, and the 0/X'th patch is
            # often not a patch, we skip it here. (TODO, warn the user about this)
            if re.search(r" 0+/[0-9]+", message.subject) and not self.is_diff(body):
                continue

            poster = parseaddr(message.poster)[0]
            date = ctime(mktime(parsedate(message.date)))
            lines.append("From %s %s" % (poster, date))

            lines.append("From: %s" % message.poster)
            lines.append("Subject: %s" % message.subject)
            lines.append("Date: %s" % message.date)
            lines.append("Message-Id: %s" % message.msg_id)
            lines.append("Xref: %s %s:%s" % (self.server, self.group, message.number))
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
            for message in self.xover(start_id, next_id):
                yield message

            start_id = next_id + 1

    def xover(self, begin, end):
        """
            Get the headers for the messages with numbers between begin and end.
        """
        if begin == end:
            return []

        _, result = self.conn.xover(str(min(begin, end)), str(max(begin, end)))

        result = [Message(int(number), subject, poster, date, msg_id, references) for
                  (number, subject, poster, date, msg_id, references, size, lines) in result]

        return sorted(result, key=lambda x: x.number)

def main():
    parser = ArgumentParser(description="""
        git fetch-series downloads a patch series from usenet.
    """, epilog="""
    NOTE: If your usenet server is hard to connect to, set up a netrc(5) file.
    """, prog="git get-series",
    usage="git fetch-series [-s SERVER] [-n NEWSGROUP] ID | git am")

    parser.add_argument('-s', '--server', default='news.gmane.org',
            help="Which news server to connect to.")
    parser.add_argument('-n', '--newsgroup', default='gmane.comp.version-control.git',
            help="Which newsgroup to read out of")
    parser.add_argument('id', help="The message id or number of the first message in the patch series")


    opts = parser.parse_args()

    try:
        a = Archive(opts.newsgroup, opts.server)
        print a.get_patch_series(opts.id)
    except NNTPError as e:
        stderr.write("NNTP %s: %s\n" % (opts.server, e.message))
    except FatalError as e:
        stderr.write("fatal: %s\n" % e.message)
    except KeyboardInterrupt:
        pass

if __name__  == "__main__":
    main()
