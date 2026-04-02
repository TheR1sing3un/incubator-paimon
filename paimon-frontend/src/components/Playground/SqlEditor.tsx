/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { useCallback } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { keymap } from '@codemirror/view';

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
  height?: string;
}

export default function SqlEditor({ value, onChange, onExecute, height = '200px' }: SqlEditorProps) {
  const executeKeymap = keymap.of([
    {
      key: 'Mod-Enter',
      run: () => {
        onExecute();
        return true;
      },
    },
  ]);

  const handleChange = useCallback(
    (val: string) => {
      onChange(val);
    },
    [onChange]
  );

  return (
    <CodeMirror
      value={value}
      height={height}
      extensions={[sql(), executeKeymap]}
      onChange={handleChange}
      placeholder="Enter SQL query... (Cmd/Ctrl+Enter to execute)"
      style={{ fontSize: 14, border: '1px solid #d9d9d9', borderRadius: 6 }}
    />
  );
}
