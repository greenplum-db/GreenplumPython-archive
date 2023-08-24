# Contributing

We warmly welcome and appreciate contributions from the community! By participating you agree to the [code of conduct](https://github.com/greenplum-db/gpupgrade/blob/main/CODE-OF-CONDUCT.md).

## Development 
- Gather input early and often rather than waiting until the end. 
  - Have in-person conversations.
  - Regularly share your branch of work.
  - Consider making a draft PR.
  - Pair as needed.
- Prefer short names based on context such as: file vs. database_file
  - People will be familiar with the code, so err on the side of brevity but avoid extremes.
- Generally follow surrounding code style and conventions. Use `make lint`.
- Have tests including unit and end-to-end.
- Resources:
  - [Protocol Buffers Syle Guide](https://developers.google.com/protocol-buffers/docs/style)
  - [BASH Style Guide](https://google.github.io/styleguide/shellguide.html)
  - [Refactoring by Martin Fowler](https://martinfowler.com/books/refactoring.html) including the [Refactoring website](https://refactoring.com/). 

## Contributing/Submitting a Pull Request 
- Sign our [Contributor License Agreement](https://cla.vmware.com/cla/1/preview).

- Fork the repository on GitHub.

- Clone the repository.

- Follow the README to set up your environmen.

- Create a change

    - Create a topic branch.

    - Make commits as logical units for ease of reviewing.

    - Try and follow similar coding styles as found throughout the code base.

    - Rebase with main often to stay in sync with upstream.

    - Ensure a well written commit message as explained [here](https://chris.beams.io/posts/git-commit/) and [here](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).

- Submit a pull request (PR).

    - Create a [pull request from your fork](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/.creating-a-pull-request-from-a-fork).

## Code Reviews 
- Follow [Google's Code Review Guidelines](https://google.github.io/eng-practices/review/reviewer/)
- PR comments should have technical explanations.
- Avoid “I prefer it this way”. See [Principles Section](https://google.github.io/eng-practices/review/reviewer/standard.html).
- Avoid these [Toxic Behaviors](https://medium.com/@sandya.sankarram/unlearning-toxic-behaviors-in-a-code-review-culture-b7c295452a3c) ([video](https://www.youtube.com/watch?v=QIUwGa-MttQ))
- Use Github's "Request changes" very sparingly. This indicates that there are critical blockers that absolutely must change before approval.
- Use Github's “Start a review” feature to submit multiple comments into a single review.
- Address PR comments with fixup or squash commits. This makes it easier for the review to see what changed.
  - Ideally wait until the PR has been approved to squash these commits, but sometimes it might be cleaner and easier to follow to combine them earlier.
  - Rebasing your PR with main is good practice.
  - Use Github’s “Resolve Conversation” button to indicate you addressed the feedback. There is no need for a comment unless you deviated from the reviewer's specific feedback.

# Community

Connect with Greenplum on:
- [Slack](https://greenplum.slack.com/)
- [gpdb-dev mailing list](https://groups.google.com/a/greenplum.org/forum/#!forum/gpdb-dev/join)
