// Copyright 2023 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common.config;

import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;

/** Command-line options definition for Worker. */
public class BuildfarmOptions extends OptionsBase {
  @Option(name = "help", abbrev = 'h', help = "Prints usage info.", defaultValue = "true")
  public boolean help;

  @Option(
      name = "prometheus_port",
      help = "Port for the prometheus service. '0' will disable prometheus hosting",
      defaultValue = "-1")
  public int prometheusPort;

  @Option(
      name = "redis_uri",
      help = "URI for Redis connection. Use 'redis://' or 'rediss://' for the scheme",
      defaultValue = "")
  public String redisUri;

  @Option(
      name = "port",
      abbrev = 'p',
      help = "Port for the buildfarm service.",
      defaultValue = "-1")
  public int port;
}
