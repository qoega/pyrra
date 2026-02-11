import React, {useEffect, useLayoutEffect, useRef, useState} from 'react'
import {Spinner} from 'react-bootstrap'
import UplotReact from 'uplot-react'
import uPlot, {AlignedData} from 'uplot'
import {ObjectiveType} from '../../App'
import {IconExternal} from '../Icons'
import {reds} from './colors'
import {seriesGaps} from './gaps'
import {PromiseClient} from '@connectrpc/connect'
import {ObjectiveService} from '../../proto/objectives/v1alpha1/objectives_connect'
import {Timestamp} from '@bufbuild/protobuf'
import {Series} from '../../proto/objectives/v1alpha1/objectives_pb'
import {selectTimeRange} from './selectTimeRange'
import {Labels, labelValues, labelsString} from '../../labels'
import {buildExternalHRef, externalName} from '../../external'

interface ErrorsGraphProps {
  client: PromiseClient<typeof ObjectiveService>
  type: ObjectiveType
  labels: Labels
  grouping: Labels
  from: number
  to: number
  uPlotCursor: uPlot.Cursor
  updateTimeRange: (min: number, max: number, absolute: boolean) => void
  absolute: boolean
}

const ErrorsGraph = ({
  client,
  type,
  labels: objectiveLabels,
  grouping,
  from,
  to,
  uPlotCursor,
  updateTimeRange,
  absolute = false,
}: ErrorsGraphProps): JSX.Element => {
  const targetRef = useRef() as React.MutableRefObject<HTMLDivElement>

  const [width, setWidth] = useState<number>(500)
  const [data, setData] = useState<AlignedData>([])
  const [seriesLabels, setSeriesLabels] = useState<Labels[]>([])
  const [loading, setLoading] = useState<boolean>(true)
  const [error, setError] = useState<boolean>(false)
  const [queryStr, setQueryStr] = useState<string>('')

  const setWidthFromContainer = () => {
    if (targetRef?.current !== undefined && targetRef?.current !== null) {
      setWidth(targetRef.current.offsetWidth)
    }
  }

  // Set width on first render
  useLayoutEffect(setWidthFromContainer)
  // Set width on every window resize
  window.addEventListener('resize', setWidthFromContainer)

  useEffect(() => {
    setLoading(true)
    setError(false)
    client
      .graphErrors({
        expr: labelsString(objectiveLabels),
        grouping: labelsString(grouping),
        start: Timestamp.fromDate(new Date(from)),
        end: Timestamp.fromDate(new Date(to)),
      })
      .then((resp) => {
        const timeseries = resp.timeseries
        if (timeseries !== undefined && timeseries.series.length >= 2) {
          const [x, ...series] = timeseries.series
          const timestamps = x.values
          const values = series.map((s: Series) => s.values)
          setData([timestamps, ...values])

          // Parse label strings into Labels objects
          const lbls: Labels[] = timeseries.labels.map((l: string) => {
            const parsed: Labels = {}
            const stripped = l.replace(/^\{|}$/g, '')
            if (stripped !== '') {
              stripped.split(',').forEach((pair) => {
                const eqIdx = pair.indexOf('=')
                if (eqIdx > 0) {
                  const key = pair.substring(0, eqIdx).trim()
                  let value = pair.substring(eqIdx + 1).trim()
                  value = value.replace(/^"|"$/g, '')
                  parsed[key] = value
                }
              })
            }
            return parsed
          })
          setSeriesLabels(lbls)
          setQueryStr(timeseries.query)
        } else {
          setData([])
          setSeriesLabels([])
        }
      })
      .catch(() => {
        setData([])
        setSeriesLabels([])
        setError(true)
      })
      .finally(() => {
        setLoading(false)
      })
  }, [client, objectiveLabels, grouping, from, to])

  if (loading) {
    return (
      <div style={{display: 'flex', alignItems: 'baseline', justifyContent: 'space-between'}}>
        <h4 className="graphs-headline">
          Errors
          <Spinner
            animation="border"
            style={{
              marginLeft: '1rem',
              marginBottom: '0.5rem',
              width: '1rem',
              height: '1rem',
              borderWidth: '1px',
            }}
          />
        </h4>
      </div>
    )
  }

  if (error) {
    return (
      <UplotReact
        options={{
          width: width,
          height: 150,
          padding: [15, 0, 0, 0],
          series: [{}, {}],
          scales: {
            x: {min: from / 1000, max: to / 1000},
            y: {min: 0, max: 1},
          },
        }}
        data={[[], []]}
      />
    )
  }

  let headline = 'Errors'
  let description: string
  switch (type) {
    case ObjectiveType.Ratio:
      description = 'What percentage of requests were errors?'
      break
    case ObjectiveType.Latency:
    case ObjectiveType.LatencyNative:
      headline = 'Too Slow'
      description = 'What percentage of requests were too slow?'
      break
    case ObjectiveType.BoolGauge:
      description = 'What percentage of probes were errors?'
      break
  }

  return (
    <>
      <div style={{display: 'flex', alignItems: 'baseline', justifyContent: 'space-between'}}>
        <h4 className="graphs-headline">{headline}</h4>
        {queryStr !== '' ? (
          <a
            className="external-prometheus"
            target="_blank"
            rel="noreferrer"
            href={buildExternalHRef([queryStr], from, to)}>
            <IconExternal height={20} width={20} />
            {externalName()}
          </a>
        ) : (
          <></>
        )}
      </div>
      <div>
        <p>{description}</p>
      </div>

      <div ref={targetRef}>
        {data.length > 0 ? (
          <UplotReact
            options={{
              width: width,
              height: 150,
              padding: [15, 0, 0, 0],
              cursor: uPlotCursor,
              series: [
                {},
                ...seriesLabels.map((label: Labels, i: number): uPlot.Series => {
                  return {
                    min: 0,
                    stroke: `#${reds[i]}`,
                    label: labelValues(label)[0],
                    gaps: seriesGaps(from / 1000, to / 1000),
                    value: (u, v) => (v == null ? '-' : (100 * v).toFixed(2) + '%'),
                  }
                }),
              ],
              scales: {
                x: {min: from / 1000, max: to / 1000},
                y: {
                  range: {
                    min: absolute ? {hard: 0, mode: 1, soft: 0} : {hard: 0},
                    max: absolute ? {hard: 1, mode: 1, soft: 1} : {hard: 1},
                  },
                },
              },
              axes: [
                {},
                {
                  values: (uplot: uPlot, v: number[]) =>
                    v.map((v: number) => `${(100 * v).toFixed(0)}%`),
                },
              ],
              hooks: {
                setSelect: [selectTimeRange(updateTimeRange)],
              },
            }}
            data={data}
          />
        ) : (
          <UplotReact
            options={{
              width: width,
              height: 150,
              padding: [15, 0, 0, 0],
              series: [{}, {}],
              scales: {
                x: {min: from / 1000, max: to / 1000},
                y: {min: 0, max: 1},
              },
            }}
            data={[[], []]}
          />
        )}
      </div>
    </>
  )
}

export default ErrorsGraph
