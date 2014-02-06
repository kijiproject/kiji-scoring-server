Kiji Scoring Server ${project.version}
===========================
The Kiji Scoring Server provides an environment for real time remote execution of trained models.
The scoring server runs as a standalone web application that, when hooked up to
KijiScoring, provides clients with the ability to retreive fresh results depending on the
freshness policy defined on the column.

The Kiji Scoring Server is a standalone web application that will scan a model repository for
updates and will either deploy a new model and make it available for scoring OR
remove an already deployed model. This behavior is controlled by the "production-ready"
flag in the model repository and can be changed by using the model repository command line tools.
This ensures that models that have been deployed to the repository but haven't been trained
yet are not made available for scoring.

Development Warning
-------------------

This project is still under heavy development and has not yet had a formal release.
The APIs and code in this project may change in severely incompatible ways while we
redesign components for their presentation-ready form.

End users are advised to not depend on any functionality in this repository until a
release is performed. See [the Kiji project homepage](http://www.kiji.org) to download
an existing release of KijiSchema, and follow [@kijiproject](http://twitter.com/kijiproject)
for announcements of future releases, including Kiji Scoring Server.

Issues are being tracked at [the Kiji JIRA instance](https://jira.kiji.org/).

Pre-Requisites
--
It's assumed that the instructions for the https://github.com/kijiproject/kiji-model-repository
have been followed with regards to setting up a new model repostory and deploying the sample
models.

Installation
--
1. Unpack the scoring server tarball into /opt/wibi/kiji-scoring-server. Alternatively, you can
unpack the tarball and create a symlink named kiji-scoring-server pointing at the folder created by
unpacking the tarball. If you are running in the BentoBox, you can access the scoring server by
going to the ${BENTO_HOME}/scoring-server folder.
2. Change the contents of the conf/configuration.json to point at the right instance,
configure the right port to run the HTTP server on and how often to scan the model
repository (in seconds) for changes.
3. Launch the scoring server by executing bin/kiji-scoring-server. Alternatively, the scoring server
can be launched as a service by copying the bin/kiji-scoring-server.initd as
/etc/init.d/kiji-scoring-server. If this is the case, then make sure that the kiji-scoring-server
folder is owned by the kiji user.
4. Currently, logs are written to the kiji-scoring-server/logs/console.out. This will change in
future releases to better support proper logging.

Usage
--
1. As mentioned, the scoring server will poll the model repository for changes. Only models
where the production-ready flags are set to true will be deployed to the scoring server. To make
an already deployed model ready for deployment, execute:
<pre>
  kiji model-repo update org.kiji.my-model-1.0.0 --production-ready=true --message="Making this model available for scoring".
</pre>
2. After a few seconds, the model's artifact will be fetched from the model repository
and deployed into the scoring server and made available for scoring. The url for applying the model
named "org.kiji.my-model" with version 1.0.0 to the entity "ophie@kiji.org"
is http://localhost:8080/models/org/kiji/my-model/1.0.0/?eid=%22ophie@kiji.org%22. Normally this
URL would be accessed by the FreshKijiTableReader; however, this can be accessed in the browser
to see the behavior of the scoring server.
3. To disable this model from the scoring server, update the production-ready flag
to false by executing:
<pre>
  kiji model-repo update org.kiji.my-model-1.0.0 --production-ready=false --message="Disabling this model from the scoring server."
</pre>
4. Now when you try to access http://localhost:8080/models/org/kiji/my-model/1.0.0/?eid=%22ophie@kiji.org%22 you should
get a 404 not found.
