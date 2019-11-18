/*
 * Copyright 2020 THL A29 Limited, a Tencent company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package util

import "fmt"

const (
	// PodGroupAnn is not used now
	PodGroupAnn = "group.batch.scheduler.tencent.com"
	// PodGroupLabel is the default label of batch scheduler
	PodGroupLabel = "group.batch.scheduler.tencent.com"
)

var (
	// ErrorNotMatched means pod does not match batch scheduling
	ErrorNotMatched = fmt.Errorf("not match batch scheduling")
	// ErrorWaiting means pod number does not match the min pods required
	ErrorWaiting = fmt.Errorf("waiting")
	// ErrorResourceNotEnough means cluster resource is not enough, mainly used in Pre-Filter
	ErrorResourceNotEnough = fmt.Errorf("resource not enough")
)
