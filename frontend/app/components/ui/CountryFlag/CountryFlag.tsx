import React, { FC, CSSProperties, memo } from 'react';
import cn from 'classnames';
import { Icon, TextEllipsis } from 'UI';
import { Tooltip } from 'antd';
import { countries } from 'App/constants';

interface CountryFlagProps {
  userCity?: string;
  userState?: string;
  country?: string;
  className?: string;
  style?: CSSProperties;
  width?: number;
  height?: number;
}


const CountryFlag: FC<CountryFlagProps> = ({
                                             userCity = '',
                                             userState = '',
                                             country = '',
                                             className = '',
                                             style = {},
                                             width = 22,
                                             height = 15
                                           }) => {
  const knownCountry = !!country && country !== 'UN';
  const countryFlag = knownCountry ? country.toLowerCase() : '';
  const countryName = knownCountry ? countries[country] : 'Unknown Country';


  const displayGeoInfo = userCity || userState || countryName;

  // display full geo info, check each part if not null, display as string
  const fullGeoInfo = [userCity, userState, countryName].filter(Boolean).join(', ');

  const renderUnknownCountry = (
    <div className='flex items-center w-full'>
      <Icon name='flag-na' size={22} className='' />
      <div className='ml-2 leading-none' style={{ whiteSpace: 'nowrap' }}>
        Unknown Country
      </div>
    </div>
  );

  const renderGeoInfo = displayGeoInfo && (
    <span className='mx-1'>
      <TextEllipsis text={displayGeoInfo} maxWidth='150px' />
    </span>
  );

  return (
    <div className='flex items-center' style={style}>
      {knownCountry ? (
        <Tooltip title={fullGeoInfo} mouseEnterDelay={0.5}>
          <div
            className={cn(`flag flag-${countryFlag}`, className)}
            style={{ width: `${width}px`, height: `${height}px` }}
          />
        </Tooltip>
      ) : (
        renderUnknownCountry
      )}
      {renderGeoInfo}
    </div>
  );
};

CountryFlag.displayName = 'CountryFlag';

export default memo(CountryFlag);
