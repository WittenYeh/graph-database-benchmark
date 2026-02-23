# Copyright 2026 Weitang Ye
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bash visualize/visualize.sh batchsize \
    --database neo4j,janusgraph,orientdb,sqlg,arangodb \
    --workload batchsize_comparison \
    --dataset coAuthorsDBLP

bash visualize/visualize.sh performance \
    --database neo4j,janusgraph,orientdb,sqlg,arangodb,aster \
    --workload medium_structural_workload \
    --dataset delaunay_n21,cit-Patents,coAuthorsDBLP

bash deploy/deploy.sh